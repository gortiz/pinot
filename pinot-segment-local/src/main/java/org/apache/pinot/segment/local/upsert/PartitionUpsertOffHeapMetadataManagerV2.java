package org.apache.pinot.segment.local.upsert;

import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.commons.lang.NotImplementedException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMultiColForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionUpsertOffHeapMetadataManagerV2 {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertOffHeapMetadataManagerV2.class);
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

  public PartitionUpsertOffHeapMetadataManagerV2(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction)
      throws Exception {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;
    String segmentName = StringUtil.join("-", "upsert_metadata", _tableNameWithType, String.valueOf(_partitionId),
        String.valueOf(System.currentTimeMillis()));

    // TODO: How to determine the cardinality of primary keys so as to estimate file size
    _memoryManager =
        new MmapMemoryManager(Files.createTempDirectory("off-heap-upsert").toAbsolutePath().toString(), segmentName,
            null);
    _bytesOffHeapMutableDictionary = new BytesOffHeapMutableDictionary(3000, 3, _memoryManager, null, 10);
    //_mutableForwardIndex = new VarByteSVMutableForwardIndex(FieldSpec.DataType.BYTES, _memoryManager, null, 3000, 16);
    _mutableForwardIndex =
        new FixedByteSVMultiColForwardIndex(false, 100, _memoryManager, null, new FieldSpec.DataType[]{
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
      int primaryKeyId = _bytesOffHeapMutableDictionary.index(recordInfo.getPrimaryKey().asBytes());
      RecordLocationRef currentRecordLocation = getRecordInfo(primaryKeyId);

      if (currentRecordLocation != null) {
        // Existing primary key
        IndexSegment currentSegment = (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentRef());
        if(currentSegment == null) {
          System.out.println("Asas");
        }
        int comparisonResult =
            recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue());

        // The current record is in the same segment
        // Update the record location when there is a tie to keep the newer record. Note that the record info
        // iterator will return records with incremental doc ids.
        if (segment == currentSegment) {
          if (comparisonResult >= 0) {
            validDocIds.replace(currentRecordLocation.getDocId(), recordInfo.getDocId());
            RecordLocationRef newRecordLocationRef =
                new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
            updateRecordInfo(primaryKeyId, newRecordLocationRef);
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
            RecordLocationRef newRecordLocationRef =
                new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
            updateRecordInfo(primaryKeyId, newRecordLocationRef);
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
          RecordLocationRef newRecordLocationRef =
              new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
          updateRecordInfo(primaryKeyId, newRecordLocationRef);
        } else {
          //updateRecordInfo(primaryKeyId, currentRecordLocation);
        }
      } else {
        // New primary key
        validDocIds.add(recordInfo.getDocId());
        RecordLocationRef newRecordLocationRef =
            new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
        updateRecordInfo(primaryKeyId, newRecordLocationRef);
      }
    }
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

    int primaryKeyId = _bytesOffHeapMutableDictionary.index(recordInfo.getPrimaryKey().asBytes());

    RecordLocationRef value = getRecordInfo(primaryKeyId);

    if (value != null) {
      RecordLocationRef currentRecordLocation = value;
      if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
        IndexSegment currentSegment = (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentRef());
        int currentDocId = currentRecordLocation.getDocId();
        if (segment == currentSegment) {
          validDocIds.replace(currentDocId, recordInfo.getDocId());
        } else {
          Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentDocId);
          validDocIds.add(recordInfo.getDocId());
        }
        RecordLocationRef newRecordLocationRef =
            new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
        updateRecordInfo(primaryKeyId, newRecordLocationRef);
      } else {
        updateRecordInfo(primaryKeyId, currentRecordLocation);
      }
    } else {
      validDocIds.add(recordInfo.getDocId());
      RecordLocationRef recordLocationRef =
          new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
      updateRecordInfo(primaryKeyId, recordLocationRef);
    }
  }

  /**
   * Removes the upsert metadata for the given immutable segment. No need to remove the upsert metadata for the
   * consuming segment because it should be replaced by the committed segment.
   */
  public void removeSegment(IndexSegment segment) {
    throw new NotImplementedException();
  }

  //TODO: How to update data for primary key at a specific index
  private void updateRecordInfo(int primaryKeyId, RecordLocationRef recordLocationRef) {
    _mutableForwardIndex.setInt(primaryKeyId, 0, recordLocationRef.getSegmentRef().intValue());
    _mutableForwardIndex.setInt(primaryKeyId, 1, recordLocationRef.getDocId());
    _mutableForwardIndex.setLong(primaryKeyId, 2, recordLocationRef.getComparisonValue());
  }

  private RecordLocationRef getRecordInfo(int primaryKeyId) {
    int segmentRef = _mutableForwardIndex.getInt(primaryKeyId, 0);

    //TODO: insert -1 as segment ref when a primary key is removed, will lead to fragmentation in the buffer though
    if(segmentRef <= INITIAL_SEGMENT_ID || segmentRef > INITIAL_SEGMENT_ID + _segmentToSegmentIdMap.size()) {
      return null;
    }

    int docId =  _mutableForwardIndex.getInt(primaryKeyId, 1);
    long comparisonVal =  _mutableForwardIndex.getLong(primaryKeyId, 2);

    return new RecordLocationRef(segmentRef, docId, comparisonVal);
  }
}
