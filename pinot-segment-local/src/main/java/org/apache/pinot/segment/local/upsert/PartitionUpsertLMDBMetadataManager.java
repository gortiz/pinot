package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Joiner;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.lang.NotImplementedException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.lmdbjava.DbiFlags.MDB_CREATE;


@NotThreadSafe
public class PartitionUpsertLMDBMetadataManager implements IPartitionUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertLMDBMetadataManager.class);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final HashFunction _hashFunction;
  private final ConcurrentHashMap<Object, Integer> _segmentToSegmentIdMap = new ConcurrentHashMap<>();
  //need to create a second reverse lookup hashmap, any way to avoid it?
  private final ConcurrentHashMap<Integer, Object> _segmentIdToSegmentMap = new ConcurrentHashMap<>();
  private final AtomicInteger _segmentId = new AtomicInteger();

  private final Dbi<ByteBuffer> _lmdb;
  final Env<ByteBuffer> _env;
  final ByteBuffer _keyBuffer;
  final ByteBuffer _valueBuffer;

  public PartitionUpsertLMDBMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction)
      throws Exception {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;

    //TODO: Needs to be changed to a configurable location
    String dirPrefix = Joiner.on("_").join(PartitionUpsertLMDBMetadataManager.class.getSimpleName(), _tableNameWithType, _partitionId);
    Path tmpDBFile = Files.createTempFile(dirPrefix+"1212",".db");
    LOGGER.info("Using storage path for mapDB {}", tmpDBFile);

    Path dbPath = Files.createTempDirectory(PartitionUpsertLMDBMetadataManager.class.getSimpleName() + "_" + System.currentTimeMillis());

    // We always need an Env. An Env owns a physical on-disk storage file. One
    // Env can store many different databases (ie sorted maps).
    _env = Env
        .create()
        // LMDB also needs to know how large our DB might be. Over-estimating is OK.
        .setMapSize(200 * 1024 * 1024)
        // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
        .setMaxDbs(1)
        // Now let's open the Env. The same path can be concurrently opened and
        // used in different processes, but do not open the same path twice in
        // the same process at the same time.
        .open(dbPath.toFile());


    _lmdb = _env.openDbi("upsert.db", MDB_CREATE);

    _keyBuffer = ByteBuffer.allocateDirect(_env.getMaxKeySize());
    _valueBuffer = ByteBuffer.allocateDirect(16);
  }


  /**
   * Initializes the upsert metadata for the given immutable segment.
   */
  @Override
  public void addSegment(IndexSegment segment, Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    int segmentId = getSegmentId(segment);

    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      try(Txn<ByteBuffer> txn = _env.txnWrite()) {
        byte[] key = recordInfo.getPrimaryKey().asBytes();
        _keyBuffer.put(key).flip();
        ByteBuffer value =  _lmdb.get(txn, _keyBuffer);

        if (value != null) {
          RecordLocationWithSegmentId currentRecordLocation = RecordLocationSerDe.deserialize(value.array());

          // Existing primary key
          IndexSegment currentSegment = (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentId());
          int comparisonResult = recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue());

          // The current record is in the same segment
          // Update the record location when there is a tie to keep the newer record. Note that the record info
          // iterator will return records with incremental doc ids.
          if (segment == currentSegment) {
            if (comparisonResult >= 0) {
              validDocIds.replace(currentRecordLocation.getDocId(), recordInfo.getDocId());
              RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                  new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
              _valueBuffer.put(RecordLocationSerDe.serialize(newRecordLocationWithSegmentId)).flip();
              _lmdb.put(txn, _keyBuffer, _valueBuffer);
              continue;
            } else {
              continue;
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
              _valueBuffer.put(RecordLocationSerDe.serialize(newRecordLocationWithSegmentId)).flip();
              _lmdb.put(txn, _keyBuffer, _valueBuffer);
              continue;
            } else {
              continue;
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
            _valueBuffer.put(RecordLocationSerDe.serialize(newRecordLocationWithSegmentId)).flip();
            _lmdb.put(txn, _keyBuffer, _valueBuffer);
          } else {
            //
          }
        } else {
          // New primary key
          validDocIds.add(recordInfo.getDocId());
          RecordLocationWithSegmentId newRecordLocationWithSegmentId =
              new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
          _valueBuffer.put(RecordLocationSerDe.serialize(newRecordLocationWithSegmentId)).flip();
          _lmdb.put(txn, _keyBuffer, _valueBuffer);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment.
   */
  @Override
  public void addRecord(IndexSegment segment, RecordInfo recordInfo) {
    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());

    try (Txn<ByteBuffer> txn = _env.txnWrite()) {
      byte[] key = recordInfo.getPrimaryKey().asBytes();

      // keyMayExist does a probablistic check for a key, can return false positives but no false negatives
      // helps to avoid expensive gets if key doesn't exist in DB
      _keyBuffer.put(key).flip();
      ByteBuffer value =  _lmdb.get(txn, _keyBuffer);
      if (value.array() != null) {
          RecordLocationWithSegmentId currentRecordLocation = RecordLocationSerDe.deserialize(value.array());
          if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
            IndexSegment currentSegment =
                (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentId());
            int currentDocId = currentRecordLocation.getDocId();
            if (segment == currentSegment) {
              validDocIds.replace(currentDocId, recordInfo.getDocId());
            } else {
              Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentDocId);
              validDocIds.add(recordInfo.getDocId());
            }
            int segmentId = getSegmentId(segment);
            RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
            _valueBuffer.put(RecordLocationSerDe.serialize(newRecordLocationWithSegmentId)).flip();
            _lmdb.put(txn, _keyBuffer, _valueBuffer);
          } else {
            //txn.put(key, RecordLocationSerDe.serialize(currentRecordLocation));
          }
      } else {
        validDocIds.add(recordInfo.getDocId());
        int segmentId = getSegmentId(segment);
        RecordLocationWithSegmentId newRecordLocationWithSegmentId =
            new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
        _valueBuffer.put(RecordLocationSerDe.serialize(newRecordLocationWithSegmentId)).flip();
        _lmdb.put(txn, _keyBuffer, _valueBuffer);
      }
    } catch (Exception ex) {
      // log error
      ex.printStackTrace();
    }
  }

  private int getSegmentId(IndexSegment segment) {
    int segmentId = _segmentToSegmentIdMap.computeIfAbsent(segment, (segmentObj) -> {
      Integer newSegmentId = _segmentId.incrementAndGet();
      _segmentIdToSegmentMap.put(newSegmentId, segment);
      return newSegmentId;
    });
    return segmentId;
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
    _lmdb.close();
    _env.close();
  }

  public static void main(String[] args) {

  }
}
