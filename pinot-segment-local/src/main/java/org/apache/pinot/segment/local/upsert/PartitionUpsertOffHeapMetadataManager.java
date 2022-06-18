package org.apache.pinot.segment.local.upsert;

import java.io.File;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MutableOffHeapByteArrayStore;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.OffHeapMutableBytesStore;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionUpsertOffHeapMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertOffHeapMetadataManager.class);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final HashFunction _hashFunction;

  private BytesOffHeapMutableDictionary _bytesOffHeapMutableDictionary;
  private MutableOffHeapByteArrayStore _mutableOffHeapByteArrayStore;
  private PinotDataBufferMemoryManager _memoryManager;
  final ConcurrentHashMap<Object, Integer> _segmentToSegmentIdMap = new ConcurrentHashMap<>();
  //need to create a second reverse lookup hashmap, any way to avoid it?
  final ConcurrentHashMap<Integer, Object> _segmentIdToSegmentMap = new ConcurrentHashMap<>();
  final AtomicInteger _segmentId = new AtomicInteger();

  public PartitionUpsertOffHeapMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction)
      throws Exception {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;
    String segmentName = StringUtil.join("-", "upsert_metadata", _tableNameWithType, String.valueOf(_partitionId),
        String.valueOf(System.currentTimeMillis()));

    _memoryManager =
        new MmapMemoryManager(Files.createTempDirectory("off-heap-upsert").toAbsolutePath().toString(), segmentName,
            null);
    _bytesOffHeapMutableDictionary = new BytesOffHeapMutableDictionary(3000, 3, _memoryManager, null, 10);
    _mutableOffHeapByteArrayStore = new MutableOffHeapByteArrayStore(_memoryManager, null, 1000, 100);
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

    byte[] value = _mutableOffHeapByteArrayStore.get(primaryKeyId);

    if (value != null) {
      RecordLocationRef currentRecordLocation = RecordLocationSerDe.deserialize(value);
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
        updateRecordInfo(primaryKeyId, RecordLocationSerDe.serialize(newRecordLocationRef));
      } else {
        updateRecordInfo(primaryKeyId, RecordLocationSerDe.serialize(currentRecordLocation));
      }
    } else {
      validDocIds.add(recordInfo.getDocId());
      RecordLocationRef recordLocationRef =
          new RecordLocationRef(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
      updateRecordInfo(primaryKeyId, RecordLocationSerDe.serialize(recordLocationRef));
    }
  }

  //TODO: How to update data for primary key at a specific index
  private void updateRecordInfo(int primaryKeyId, byte[] recordLocationRef) {

  }

  public static void main(String[] args) {

  }
}
