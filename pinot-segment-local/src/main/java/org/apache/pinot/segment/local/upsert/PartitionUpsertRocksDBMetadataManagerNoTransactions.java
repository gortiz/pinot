package org.apache.pinot.segment.local.upsert;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionUpsertRocksDBMetadataManagerNoTransactions implements IPartitionUpsertMetadataManager  {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertRocksDBMetadataManagerNoTransactions.class);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final HashFunction _hashFunction;
  private final RocksDB _rocksDB;
  final ConcurrentHashMap<Object, Integer> _segmentToSegmentIdMap = new ConcurrentHashMap<>();
  //need to create a second reverse lookup hashmap, any way to avoid it?
  final ConcurrentHashMap<Integer, Object> _segmentIdToSegmentMap = new ConcurrentHashMap<>();
  final AtomicInteger _segmentId = new AtomicInteger();
  private final ReadOptions _readOptions;
  private byte[] _reuse = new byte[16];

  static {
    RocksDB.loadLibrary();
  }

  public PartitionUpsertRocksDBMetadataManagerNoTransactions(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction)
      throws Exception {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;
    File file = new File("/Users/kharekartik/Documents/Developer/incubator-pinot/upsert/rocksDB/" + _tableNameWithType + "/" + System.currentTimeMillis());
    file.mkdirs();
    System.out.println("USING PATH: " + file.getAbsolutePath());
    Options dbOptions = new Options();
    dbOptions.setCreateIfMissing(true);
    dbOptions.setBloomLocality(10);
    BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setFilterPolicy(new BloomFilter(10));
    dbOptions.setTableFormatConfig(blockBasedTableConfig);

    _rocksDB = RocksDB.open(dbOptions,
        file.toPath().toAbsolutePath().toString());

    _readOptions = new ReadOptions();
  }

  /**
   * Initializes the upsert metadata for the given immutable segment.
   */
  @Override
  public void addSegment(IndexSegment segment, Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    LOGGER.info("Adding upsert metadata for segment: {}", segmentName);
    int segmentId = _segmentToSegmentIdMap.computeIfAbsent(segment, (segmentObj) -> {
      Integer newSegmentId = _segmentId.incrementAndGet();
      _segmentIdToSegmentMap.put(newSegmentId, segment);
      return newSegmentId;
    });

    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      try {
        byte[] key = ((String) recordInfo.getPrimaryKey().getValues()[0]).getBytes(StandardCharsets.UTF_8);
        int len = _rocksDB.get(key, _reuse);
        if (len != RocksDB.NOT_FOUND) {
          RecordLocationRef currentRecordLocation = RecordLocationSerDe.deserialize(_reuse);

          // Existing primary key
          IndexSegment currentSegment =
              (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentRef());
          int comparisonResult = recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue());

          // The current record is in the same segment
          // Update the record location when there is a tie to keep the newer record. Note that the record info
          // iterator will return records with incremental doc ids.
          if (segment == currentSegment) {
            if (comparisonResult >= 0) {
              validDocIds.replace(currentRecordLocation.getDocId(), recordInfo.getDocId());
              RecordLocationRef newRecordLocationRef =
                  new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
              _rocksDB.put(key, RecordLocationSerDe.serialize(newRecordLocationRef));
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
              RecordLocationRef newRecordLocationRef =
                  new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
              _rocksDB.put(key, RecordLocationSerDe.serialize(newRecordLocationRef));
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
            RecordLocationRef newRecordLocationRef =
                new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
            _rocksDB.put(key, RecordLocationSerDe.serialize(newRecordLocationRef));
          } else {
            //
          }
        } else {
          // New primary key
          validDocIds.add(recordInfo.getDocId());
          RecordLocationRef newRecordLocationRef =
              new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
          _rocksDB.put(key, RecordLocationSerDe.serialize(newRecordLocationRef));
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
    int segmentId = _segmentToSegmentIdMap.computeIfAbsent(segment, (segmentObj) -> {
      Integer newSegmentId = _segmentId.incrementAndGet();
      _segmentIdToSegmentMap.put(newSegmentId, segment);
      return newSegmentId;
    });

    try {
      byte[] key = ((String) recordInfo.getPrimaryKey().getValues()[0]).getBytes(StandardCharsets.UTF_8);
      int len = _rocksDB.get(key, _reuse);
      if (len != RocksDB.NOT_FOUND) {
        RecordLocationRef currentRecordLocation = RecordLocationSerDe.deserialize(_reuse);

        if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
          IndexSegment currentSegment =
              (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentRef());
          int currentDocId = currentRecordLocation.getDocId();
          if (segment == currentSegment) {
            validDocIds.replace(currentDocId, recordInfo.getDocId());
          } else {
            Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentDocId);
            validDocIds.add(recordInfo.getDocId());
          }
          RecordLocationRef newRecordLocationRef =
              new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
          _rocksDB.put(key, RecordLocationSerDe.serialize(newRecordLocationRef));
        } else {
          //txn.put(key, RecordLocationSerDe.serialize(currentRecordLocation));
        }
      } else {
        validDocIds.add(recordInfo.getDocId());
        RecordLocationRef recordLocationRef =
            new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
        _rocksDB.put(key, RecordLocationSerDe.serialize(recordLocationRef));
      }
    } catch (RocksDBException rocksDBException) {
      // log error
      //rocksDBException.printStackTrace();
    }
  }

  @Override
  public GenericRow updateRecord(GenericRow record, RecordInfo recordInfo) {
    return null;
  }

  @Override
  public void removeSegment(IndexSegment segment) {

  }

  public static void main(String[] args) {

  }
}
