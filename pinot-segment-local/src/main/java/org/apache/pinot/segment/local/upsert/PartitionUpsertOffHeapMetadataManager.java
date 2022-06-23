package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Joiner;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.commons.lang.NotImplementedException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMultiColForwardIndex;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// POC for off heap upsert. Works correctly in happy path. Doesn't support partial upsert, deletion and hashing yet.
public class PartitionUpsertOffHeapMetadataManager implements IPartitionUpsertMetadataManager  {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertOffHeapMetadataManager.class);
  public static final int INITIAL_SEGMENT_ID = 10;

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final HashFunction _hashFunction;

  public BytesOffHeapMutableDictionary _bytesOffHeapMutableDictionary;
  private PinotDataBufferMemoryManager _memoryManager;
  private FixedByteSVMultiColForwardIndex _mutableForwardIndex;
  final ConcurrentHashMap<Object, Integer> _segmentToSegmentIdMap = new ConcurrentHashMap<>();
  //need to create a second reverse lookup hashmap, any way to avoid it?
  final ConcurrentHashMap<Integer, Object> _segmentIdToSegmentMap = new ConcurrentHashMap<>();
  final AtomicInteger _segmentId = new AtomicInteger(INITIAL_SEGMENT_ID);

  // TODO: figure out optimisation in lock usage. Might not be needed in some paths.
  private final ReentrantReadWriteLock _readWriteLock = new ReentrantReadWriteLock();
  private final ReentrantLock _dictionaryLock = new ReentrantLock();

  public PartitionUpsertOffHeapMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction)
      throws Exception {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;

    String prefix = Joiner.on("_").join(PartitionUpsertOffHeapMetadataManager.class.getSimpleName(), _tableNameWithType, _partitionId);
    Path offHeapStoreDir = Files.createTempDirectory(prefix);

    LOGGER.info("Using offHeap storage dir {}", offHeapStoreDir);
    String segmentName = StringUtil.join("-", "upsert_metadata", _tableNameWithType, String.valueOf(_partitionId),
        String.valueOf(System.currentTimeMillis()));

    // TODO: How to determine the cardinality of primary keys so as to estimate file size
    _memoryManager =
        new MmapMemoryManager(offHeapStoreDir.toAbsolutePath().toString(), segmentName,
            null);

    // Used to store mapping from primary key to an int id.
    _bytesOffHeapMutableDictionary = new BytesOffHeapMutableDictionary(50000, 3, _memoryManager, null, 10);

    //The record to be stored contains an integer segmentID (derived from segment), and integer doc id, and a long comparable.
    // The comparable data type will need to be part of the constructor
    // The offset at which the record is stored is determined by the dictId derived from the primary key using `_bytesOffHeapMutableDictionary`
    _mutableForwardIndex =
        new FixedByteSVMultiColForwardIndex(false, 10000, _memoryManager, null, new FieldSpec.DataType[]{
            FieldSpec.DataType.INT, FieldSpec.DataType.INT, FieldSpec.DataType.LONG
        });
  }

  /**
   * Initializes the upsert metadata for the given immutable segment.
   */
  public void addSegment(IndexSegment segment, Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    int segmentId = _segmentToSegmentIdMap.computeIfAbsent(segment, (segmentObj) -> {
      Integer newSegmentId = _segmentId.incrementAndGet();
      _segmentIdToSegmentMap.put(newSegmentId, segment);
      return newSegmentId;
    });
    LOGGER.info("Adding upsert metadata for segment: {}", segmentName);

    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      int primaryKeyId = getPrimaryKeyId(recordInfo);
      RecordLocationWithSegmentId currentRecordLocation = getRecordInfo(primaryKeyId);


      if (currentRecordLocation != null) {
        // Existing primary key
        IndexSegment currentSegment = (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentId());
        int comparisonResult =
            recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue());

        // The current record is in the same segment
        // Update the record location when there is a tie to keep the newer record. Note that the record info
        // iterator will return records with incremental doc ids.
        if (segment == currentSegment) {
          if (comparisonResult >= 0) {
            validDocIds.replace(currentRecordLocation.getDocId(), recordInfo.getDocId());
            RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
            updateRecordInfo(primaryKeyId, newRecordLocationWithSegmentId);
            continue;
          } else {
            continue;
            //updateRecordInfo(primaryKeyId, currentRecordLocation);
          }
        }

        // The current record is in an old segment being replaced
        // This could happen when committing a consuming segment, or reloading a completed segment. In this
        // case, we want to update the record location when there is a tie because the record locations should
        // point to the new added segment instead of the old segment being replaced. Also, do not update the valid
        // doc ids for the old segment because it has not been replaced yet.
        String currentSegmentName = currentSegment.getSegmentName();
        if (segmentName.equals(currentSegmentName)) {
          if (comparisonResult >= 0) {
            validDocIds.add(recordInfo.getDocId());
            RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
            updateRecordInfo(primaryKeyId, newRecordLocationWithSegmentId);
            continue;
          } else {
            continue;
            //updateRecordInfo(primaryKeyId, currentRecordLocation);
          }
        }

        // The current record is in a different segment
        // Update the record location when getting a newer comparison value, or the value is the same as the
        // current value, but the segment has a larger sequence number (the segment is newer than the current
        // segment).
        if (comparisonResult > 0) {
          Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentRecordLocation.getDocId());
          validDocIds.add(recordInfo.getDocId());
          RecordLocationWithSegmentId newRecordLocationWithSegmentId =
              new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
          updateRecordInfo(primaryKeyId, newRecordLocationWithSegmentId);
        } else {
          //updateRecordInfo(primaryKeyId, currentRecordLocation);
        }
      } else {
        // New primary key
        validDocIds.add(recordInfo.getDocId());
        RecordLocationWithSegmentId newRecordLocationWithSegmentId =
            new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
        updateRecordInfo(primaryKeyId, newRecordLocationWithSegmentId);
      }
    }
  }

  private int getPrimaryKeyId(RecordInfo recordInfo) {
    _dictionaryLock.lock();
    byte[] pk = recordInfo.getPrimaryKey().asBytes();
    int primaryKeyId = _bytesOffHeapMutableDictionary.index(pk);
    _dictionaryLock.unlock();
    return primaryKeyId;
  }

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment.
   */
  public void addRecord(IndexSegment segment, RecordInfo recordInfo) {
    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    int segmentId = _segmentToSegmentIdMap.computeIfAbsent(segment, (segmentObj) -> {
      Integer newSegmentId = _segmentId.incrementAndGet();
      _segmentIdToSegmentMap.put(newSegmentId, segment);
      return newSegmentId;
    });

    int primaryKeyId = getPrimaryKeyId(recordInfo);

    RecordLocationWithSegmentId value = getRecordInfo(primaryKeyId);

    if (value != null) {
      RecordLocationWithSegmentId currentRecordLocation = value;
      if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
        IndexSegment currentSegment = (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentId());
        int currentDocId = currentRecordLocation.getDocId();
        if (segment == currentSegment) {
          validDocIds.replace(currentDocId, recordInfo.getDocId());
        } else {
          Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentDocId);
          validDocIds.add(recordInfo.getDocId());
        }
        RecordLocationWithSegmentId newRecordLocationWithSegmentId =
            new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
        updateRecordInfo(primaryKeyId, newRecordLocationWithSegmentId);
      } else {
        updateRecordInfo(primaryKeyId, currentRecordLocation);
      }
    } else {
      validDocIds.add(recordInfo.getDocId());
      RecordLocationWithSegmentId recordLocationWithSegmentId =
          new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
      updateRecordInfo(primaryKeyId, recordLocationWithSegmentId);
    }
  }

  private void updateRecordInfo(int primaryKeyId, RecordLocationWithSegmentId recordLocationWithSegmentId) {
    _readWriteLock.writeLock().lock();
    _mutableForwardIndex.setInt(primaryKeyId, 0, recordLocationWithSegmentId.getSegmentId().intValue());
    _mutableForwardIndex.setInt(primaryKeyId, 1, recordLocationWithSegmentId.getDocId());
    _mutableForwardIndex.setLong(primaryKeyId, 2, recordLocationWithSegmentId.getComparisonValue());
    _readWriteLock.writeLock().unlock();
  }

  private RecordLocationWithSegmentId getRecordInfo(int primaryKeyId) {
    _readWriteLock.readLock().lock();
    int segmentRef = _mutableForwardIndex.getInt(primaryKeyId, 0);

    //TODO: This is just a temp hack.
    // Insert -1 as segment ref when a primary key is removed, will lead to fragmentation in the buffer though
    if(segmentRef <= INITIAL_SEGMENT_ID || segmentRef > INITIAL_SEGMENT_ID + _segmentToSegmentIdMap.size()) {
      _readWriteLock.readLock().unlock();
      return null;
    }

    int docId =  _mutableForwardIndex.getInt(primaryKeyId, 1);
    long comparisonVal =  _mutableForwardIndex.getLong(primaryKeyId, 2);

    _readWriteLock.readLock().unlock();
    return new RecordLocationWithSegmentId(segmentRef, docId, comparisonVal);
  }

  @Override
  public GenericRow updateRecord(GenericRow record, RecordInfo recordInfo) {
    throw new NotImplementedException();
  }

  @Override
  public void removeSegment(IndexSegment segment) {
    throw new NotImplementedException();
  }

  @Override
  public void close() {
    try {
      _mutableForwardIndex.close();
      _bytesOffHeapMutableDictionary.close();
      _memoryManager.close();
    } catch (Exception e) {

    }
  }
}
