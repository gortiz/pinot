package org.apache.pinot.segment.local.upsert;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MutableOffHeapByteArrayStore;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.local.realtime.impl.dictionary.OffHeapMutableBytesStore;
import org.apache.pinot.segment.local.realtime.impl.forward.VarByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionUpsertOffHeapMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertOffHeapMetadataManager.class);
  public static final int INITIAL_SEGMENT_ID = 10;

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final HashFunction _hashFunction;

  private BytesOffHeapMutableDictionary _bytesOffHeapMutableDictionary;
  private PinotDataBufferMemoryManager _memoryManager;
  private MutableForwardIndex _mutableForwardIndex;
  final ConcurrentHashMap<Object, Integer> _segmentToSegmentIdMap = new ConcurrentHashMap<>();
  //need to create a second reverse lookup hashmap, any way to avoid it?
  final ConcurrentHashMap<Integer, Object> _segmentIdToSegmentMap = new ConcurrentHashMap<>();
  final AtomicInteger _segmentId = new AtomicInteger(INITIAL_SEGMENT_ID);

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

    // TODO: How to determine the cardinality of primary keys so as to estimate file size
    _memoryManager =
        new MmapMemoryManager(Files.createTempDirectory("off-heap-upsert").toAbsolutePath().toString(), segmentName,
            null);
    _bytesOffHeapMutableDictionary = new BytesOffHeapMutableDictionary(3000, 3, _memoryManager, null, 10);
    _mutableForwardIndex = new VarByteSVMutableForwardIndex(FieldSpec.DataType.BYTES, _memoryManager, null, 3000, 16);
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

  //TODO: How to update data for primary key at a specific index
  private void updateRecordInfo(int primaryKeyId, RecordLocationRef recordLocationRef) {
    _mutableForwardIndex.setBytes(primaryKeyId, recordLocationRef.asBytes());
  }

  private RecordLocationRef getRecordInfo(int primaryKeyId) {
    byte[] val = _mutableForwardIndex.getBytes(primaryKeyId);
    ByteBuffer buffer = ByteBuffer.wrap(val, 0, 16);
    int segmentRef = buffer.getInt();

    //TODO: insert -1 as segment ref when a primary key is removed, will lead to fragmentation in the buffer though
    if(segmentRef < INITIAL_SEGMENT_ID || segmentRef > INITIAL_SEGMENT_ID + _segmentToSegmentIdMap.size()) {
      return null;
    }

    int docId = buffer.getInt();
    long comparisonVal = buffer.getLong();

    return new RecordLocationRef(segmentRef, docId, comparisonVal);
  }
}
