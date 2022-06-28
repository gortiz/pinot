package org.apache.pinot.segment.local.upsert;

import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.HashFunction;


public class PartitionUpsertMetadataManagerFactory {

  public enum MetadataStore {
    ON_HEAP, ON_HEAP_SERDE, OFF_HEAP, ROCKSDB, MAPDB, CHRONICLE_MAP, SQLITE, H2
  }


  public static IPartitionUpsertMetadataManager getPartitionUpsertMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction, MetadataStore metadataStore){

    try {
      switch (metadataStore) {
        case ON_HEAP:
          return new PartitionUpsertMetadataManager(tableNameWithType, partitionId, serverMetrics, partialUpsertHandler,
              hashFunction);
        case ON_HEAP_SERDE:
          return new PartitionUpsertSerializedOnHeapMetadataManager(tableNameWithType, partitionId, serverMetrics, partialUpsertHandler,
              hashFunction);
        case OFF_HEAP:
          return new PartitionUpsertOffHeapMetadataManager(tableNameWithType, partitionId, serverMetrics,
              partialUpsertHandler, hashFunction);
        case ROCKSDB:
          return new PartitionUpsertRocksDBMetadataManager(tableNameWithType, partitionId, serverMetrics,
              partialUpsertHandler, hashFunction);
        case MAPDB:
          return new PartitionUpsertMapDBMetadataManager(tableNameWithType, partitionId, serverMetrics,
              partialUpsertHandler, hashFunction);
        case CHRONICLE_MAP:
          return new PartitionUpsertChronicleMapMetadataManager(tableNameWithType, partitionId, serverMetrics,
              partialUpsertHandler, hashFunction);
        case SQLITE:
          return new PartitionUpsertSQLiteMetadataManager(tableNameWithType, partitionId, serverMetrics,
              partialUpsertHandler, hashFunction);
        case H2:
          return new PartitionUpsertH2MetadataManager(tableNameWithType, partitionId, serverMetrics,
              partialUpsertHandler, hashFunction);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

}
