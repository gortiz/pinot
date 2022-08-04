package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Joiner;
import com.google.common.hash.Funnels;
import groovy.model.ValueHolder;
import java.io.Closeable;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.xml.crypto.Data;
import org.apache.commons.lang.NotImplementedException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ClockCache;
import org.rocksdb.CompressionType;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.HashLinkedListMemTableConfig;
import org.rocksdb.HashSkipListMemTableConfig;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.MemTableConfig;
import org.rocksdb.MemoryUsageType;
import org.rocksdb.MemoryUtil;
import org.rocksdb.Options;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.Statistics;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@NotThreadSafe
public class PartitionUpsertRocksDBMetadataManager implements IPartitionUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertRocksDBMetadataManager.class);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final HashFunction _hashFunction;
  private final ConcurrentHashMap<Object, Integer> _segmentToSegmentIdMap = new ConcurrentHashMap<>();
  //need to create a second reverse lookup hashmap, any way to avoid it?
  private final ConcurrentHashMap<Integer, Object> _segmentIdToSegmentMap = new ConcurrentHashMap<>();
  private final AtomicInteger _segmentId = new AtomicInteger();
  private final WriteOptions _writeOptions;
  private final ReadOptions _readOptions;

  private final RocksDB _rocksDB;
  private Statistics _statistics;

  public PartitionUpsertRocksDBMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction)
      throws Exception {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;

    //TODO: Needs to be changed to a configurable location
    String dirPath = Joiner.on("/").join(System.getProperty("user.dir"), _tableNameWithType, PartitionUpsertRocksDBMetadataManager.class.getSimpleName(), System.currentTimeMillis());
    File dir = new File(dirPath);
    dir.mkdirs();

    LOGGER.info("Using storage path for rocksdb {}", dir);

    Options options = getConfig();
    _rocksDB = RocksDB.open(options, dir.getAbsolutePath());
    _readOptions = new ReadOptions();
    _writeOptions = new WriteOptions();

    // Allows for faster puts while trading off recoverability in case of failure.
    _writeOptions.setDisableWAL(true);
  }

  /**
   * #ifndef ROCKSDB_LITE
   * ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForPointLookup(
   *     uint64_t block_cache_size_mb) {
   *   BlockBasedTableOptions block_based_options;
   *   block_based_options.data_block_index_type =
   *       BlockBasedTableOptions::kDataBlockBinaryAndHash;
   *   block_based_options.data_block_hash_table_util_ratio = 0.75;
   *   block_based_options.filter_policy.reset(NewBloomFilterPolicy(10));
   *   block_based_options.block_cache =
   *       NewLRUCache(static_cast<size_t>(block_cache_size_mb * 1024 * 1024));
   *   table_factory.reset(new BlockBasedTableFactory(block_based_options));
   *   memtable_prefix_bloom_size_ratio = 0.02;
   *   memtable_whole_key_filtering = true;
   *   return this;
   * }
   * @return
   */
  private Options getOptimisedConfigForPointLookup() {
    Options dbOptions = new Options();
    dbOptions.setCreateIfMissing(true);

    BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setFormatVersion(5); // using latest file format
    blockBasedTableConfig.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash);
    blockBasedTableConfig.setDataBlockHashTableUtilRatio(0.75);
    blockBasedTableConfig.setFilterPolicy(new BloomFilter(10));
    blockBasedTableConfig.setBlockCache(new LRUCache(1024 * 1024 * 1024));

    blockBasedTableConfig.setIndexType(IndexType.kHashSearch);
    dbOptions.useFixedLengthPrefixExtractor(10);

    // Pin and Cache blocks
    blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
    blockBasedTableConfig.setPinTopLevelIndexAndFilter(true);
    blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
    blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
    blockBasedTableConfig.setOptimizeFiltersForMemory(true);

    dbOptions.setTableFormatConfig(blockBasedTableConfig);

    dbOptions.setMaxOpenFiles(-1); // allow multiple sstable files to be opened, good when you have to do random lreads
    dbOptions.setInfoLogLevel(InfoLogLevel.HEADER_LEVEL); // disable logging
    dbOptions.setStatsDumpPeriodSec(0); // disable dumping rocksDB stats in logs
    dbOptions.setUseFsync(false);
    dbOptions.setMemtableWholeKeyFiltering(true);
    dbOptions.setMemtablePrefixBloomSizeRatio(0.1);

    //Add row cache and use faster compression (default: snappy)
    dbOptions.setRowCache(new LRUCache(1024 * 1024 * 1024));
    dbOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);

    _statistics = new Statistics();
    dbOptions.setStatistics(_statistics);
    return dbOptions;
  }

  private Options getConfig() {
    Options dbOptions = new Options();
    dbOptions.setCreateIfMissing(true);

    _statistics = new Statistics();
    dbOptions.setStatistics(_statistics);
    return dbOptions;
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
      Lock lock = UpsertPrimaryKeyLock.getPrimaryKeyLock(recordInfo.getPrimaryKey());
      try {
        lock.lock();
        byte[] key = recordInfo.getPrimaryKey().asBytes();
        byte[] value = _rocksDB.get(_readOptions, key);

        if (value != null) {
          RecordLocationWithSegmentId currentRecordLocation = RecordLocationSerDe.deserialize(value);

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
              _rocksDB.put(_writeOptions, key, RecordLocationSerDe.serialize(newRecordLocationWithSegmentId));
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
              _rocksDB.put(_writeOptions, key, RecordLocationSerDe.serialize(newRecordLocationWithSegmentId));
              continue;
            } else {
              continue;
            }
          }

          // The current record is in a different segment
          // Update the record location when getting a newer comparison value, or the value is the same as the
          // current value, but the segment has a larger sequence number (the segment is newer than the current
          // segment).
          if (comparisonResult > 0 || (comparisonResult == 0 && LLCSegmentName.isLowLevelConsumerSegmentName(
              segmentName) && LLCSegmentName.isLowLevelConsumerSegmentName(currentSegmentName)
              && LLCSegmentName.getSequenceNumber(segmentName) > LLCSegmentName.getSequenceNumber(
              currentSegmentName))) {
            Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentRecordLocation.getDocId());
            validDocIds.add(recordInfo.getDocId());
            RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
            _rocksDB.put(_writeOptions, key, RecordLocationSerDe.serialize(newRecordLocationWithSegmentId));
          } else {
            //
          }
        } else {
          // New primary key
          validDocIds.add(recordInfo.getDocId());
          RecordLocationWithSegmentId newRecordLocationWithSegmentId =
              new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
          _rocksDB.put(_writeOptions, key, RecordLocationSerDe.serialize(newRecordLocationWithSegmentId));
        }
      } catch (Exception e) {
        LOGGER.warn("Could not update partition metadata in rocksDB for segment {}", segmentName, e);
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment.
   */
  @Override
  public void addRecord(IndexSegment segment, RecordInfo recordInfo) {
    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    Lock lock = UpsertPrimaryKeyLock.getPrimaryKeyLock(recordInfo.getPrimaryKey());

    try {
      lock.lock();
      byte[] key = recordInfo.getPrimaryKey().asBytes();
      byte[] value = _rocksDB.get(_readOptions, key);

      if (value != null) {

        RecordLocationWithSegmentId currentRecordLocation = RecordLocationSerDe.deserialize(value);
        if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
          IndexSegment currentSegment = (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentId());
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
          _rocksDB.put(_writeOptions, key, RecordLocationSerDe.serialize(newRecordLocationWithSegmentId));
        } else {
          //txn.put(key, RecordLocationSerDe.serialize(currentRecordLocation));
        }
      } else {
        validDocIds.add(recordInfo.getDocId());
        int segmentId = getSegmentId(segment);
        RecordLocationWithSegmentId recordLocationWithSegmentId =
            new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
        _rocksDB.put(_writeOptions, key, RecordLocationSerDe.serialize(recordLocationWithSegmentId));
      }
    } catch (RocksDBException rocksDBException) {
      // log error
      LOGGER.warn("Could not update partition metadata in rocksDB for segment {}", segment.getSegmentName(), rocksDBException);
    } finally {
      lock.unlock();
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
    try {
      Map<MemoryUsageType, Long> memoryUsageByType =  MemoryUtil.getApproximateMemoryUsageByType(Collections.singletonList(_rocksDB), Collections.emptySet());
      for(Map.Entry<MemoryUsageType, Long> kv : memoryUsageByType.entrySet()) {
        System.out.println(kv.getKey().toString() + " :  " + kv.getValue());
      }
      System.out.println(_rocksDB.getProperty("rocksdb.stats"));
      System.out.println(_statistics.toString());
    } catch (Exception e) {
        LOGGER.warn("Error occurred while fetching stats from RocksDB", e);
    }
    _rocksDB.close();
  }
}
