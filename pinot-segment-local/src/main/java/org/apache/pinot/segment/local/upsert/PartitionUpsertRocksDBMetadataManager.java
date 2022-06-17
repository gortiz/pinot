package org.apache.pinot.segment.local.upsert;

import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.utils.StringUtil;
import org.rocksdb.Options;
import org.rocksdb.OptionsUtil;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionUpsertRocksDBMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertRocksDBMetadataManager.class);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final HashFunction _hashFunction;
  private final TransactionDB _rocksDB;
  final ConcurrentHashMap<Object, Integer> _segmentToSegmentIdMap = new ConcurrentHashMap<>();
  //need to create a second reverse lookup hashmap, any way to avoid it?
  final ConcurrentHashMap<Integer, Object> _segmentIdToSegmentMap = new ConcurrentHashMap<>();
  final AtomicInteger _segmentId = new AtomicInteger();

  public PartitionUpsertRocksDBMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction)  throws Exception {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;
    String dbPath = StringUtil.join("/", "upsert_metdata", _tableNameWithType, String.valueOf(_partitionId), String.valueOf(System.currentTimeMillis()));
    _rocksDB = TransactionDB.open(new Options(), new TransactionDBOptions() ,Files.createTempDirectory(dbPath).toAbsolutePath().toString());
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

    try (Transaction txn = _rocksDB.beginTransaction(new WriteOptions())) {
      byte[] key = recordInfo.getPrimaryKey().asBytes();
      byte[] value = txn.get(new ReadOptions(), key);

      if (value != null) {
        RecordLocationRef currentRecordLocation = RecordLocationSerDe.deserialize(value);
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
          txn.put(key, RecordLocationSerDe.serialize(newRecordLocationRef));
        } else {
          txn.put(key, RecordLocationSerDe.serialize(currentRecordLocation));
        }
      } else {
        validDocIds.add(recordInfo.getDocId());
        RecordLocationRef recordLocationRef =
            new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
        txn.put(key, RecordLocationSerDe.serialize(recordLocationRef));
      }
    } catch (RocksDBException rocksDBException) {
      // log error
    }
  }

  public static void main(String[] args) {

  }
}
