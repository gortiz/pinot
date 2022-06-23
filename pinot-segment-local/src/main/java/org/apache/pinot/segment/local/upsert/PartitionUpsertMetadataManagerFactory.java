package org.apache.pinot.segment.local.upsert;

import javax.annotation.Nullable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.config.table.HashFunction;


public class PartitionUpsertMetadataManagerFactory {

  public enum MetadataStore {
    ON_HEAP, OFF_HEAP, ROCKSDB
  }


  public static IPartitionUpsertMetadataManager getPartitionUpsertMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction, MetadataStore metadataStore){

    try {
      switch (metadataStore) {
        case ON_HEAP:
          return new PartitionUpsertMetadataManager(tableNameWithType, partitionId, serverMetrics, partialUpsertHandler,
              hashFunction);
        case OFF_HEAP:
          return new PartitionUpsertOffHeapMetadataManager(tableNameWithType, partitionId, serverMetrics,
              partialUpsertHandler, hashFunction);
        case ROCKSDB:
          return new PartitionUpsertRocksDBMetadataManager(tableNameWithType, partitionId, serverMetrics,
              partialUpsertHandler, hashFunction);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

}
