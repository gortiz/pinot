package org.apache.pinot.segment.local.upsert;

import java.io.File;
import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.MmapMemoryManager;
import org.apache.pinot.segment.local.io.writer.impl.MutableOffHeapByteArrayStore;
import org.apache.pinot.segment.local.realtime.impl.dictionary.BytesOffHeapMutableDictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.HashFunction;
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

  public PartitionUpsertOffHeapMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;
   // _memoryManager = new MmapMemoryManager();
   // _bytesOffHeapMutableDictionary = new BytesOffHeapMutableDictionary(3000, 3, )
  }
  public static void main(String[] args) {

  }
}
