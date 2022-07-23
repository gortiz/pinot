/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.upsert;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import net.openhft.chronicle.map.ChronicleMap;
import org.apache.commons.lang.NotImplementedException;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the upsert metadata per partition.
 * <p>For multiple records with the same comparison value (default to timestamp), the manager will preserve the latest
 * record based on the sequence number of the segment. If 2 records with the same comparison value are in the same
 * segment, the one with larger doc id will be preserved. Note that for tables with sorted column, the records will be
 * re-ordered when committing the segment, and we will use the re-ordered doc ids instead of the ingestion doc ids to
 * decide the record to preserve.
 *
 * <p>There will be short term inconsistency when updating the upsert metadata, but should be consistent after the
 * operation is done:
 * <ul>
 *   <li>
 *     When updating a new record, it first removes the doc id from the current location, then update the new location.
 *   </li>
 *   <li>
 *     When adding a new segment, it removes the doc ids from the current locations before the segment being added to
 *     the RealtimeTableDataManager.
 *   </li>
 *   <li>
 *     When replacing an existing segment, after the record location being replaced with the new segment, the following
 *     updates applied to the new segment's valid doc ids won't be reflected to the replaced segment's valid doc ids.
 *   </li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@ThreadSafe
public class PartitionUpsertChronicleMapMetadataManager implements IPartitionUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertChronicleMapMetadataManager.class);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final HashFunction _hashFunction;

  private final ConcurrentHashMap<Object, Integer> _segmentToSegmentIdMap = new ConcurrentHashMap<>();
  //need to create a second reverse lookup hashmap, any way to avoid it?
  private final ConcurrentHashMap<Integer, Object> _segmentIdToSegmentMap = new ConcurrentHashMap<>();
  private final AtomicInteger _segmentId = new AtomicInteger();

  // TODO(upsert): consider an off-heap KV store to persist this mapping to improve the recovery speed.
  @VisibleForTesting
  final ChronicleMap<byte[], byte[]> _primaryKeyToRecordLocationMap;

  // Reused for reading previous record during partial upsert
  private final GenericRow _reuse = new GenericRow();

  public PartitionUpsertChronicleMapMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction, int numEntries) throws Exception {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;

    String dirPath = Joiner.on("/").join(System.getProperty("user.dir"), _tableNameWithType, PartitionUpsertChronicleMapMetadataManager.class.getSimpleName(), System.currentTimeMillis());
    File dir = new File(dirPath);
    dir.mkdirs();

    File file = new File(dirPath, "chronicleMap.db");
    LOGGER.info("Using storage path for chronicleMap {}", file.getPath());

    _primaryKeyToRecordLocationMap = ChronicleMap
        .of(byte[].class, byte[].class)
        .name(PartitionUpsertChronicleMapMetadataManager.class.getSimpleName() + "-" + System.currentTimeMillis())
        .averageKeySize(100.0)
        .averageValueSize(16.0)
        .entries(numEntries)
        .maxBloatFactor(2.0)
        .createPersistedTo(file);
  }

  public PartitionUpsertChronicleMapMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction) throws Exception {
    this(tableNameWithType, partitionId, serverMetrics, partialUpsertHandler, hashFunction, 11_000_000);
  }

  /**
   * Initializes the upsert metadata for the given immutable segment.
   */
  @Override
  public void addSegment(IndexSegment segment, Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    //LOGGER.info("Adding upsert metadata for segment: {}", segmentName);
    int segmentId = getSegmentId(segment);

    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      _primaryKeyToRecordLocationMap.compute(recordInfo.getPrimaryKey().asBytes(),
          (primaryKey, value) -> {
            if (value != null) {
              RecordLocationWithSegmentId currentRecordLocation = RecordLocationSerDe.deserialize(value);
              // Existing primary key
              IndexSegment currentSegment = (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentId());
              int comparisonResult =
                  recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue());

              // The current record is in the same segment
              // Update the record location when there is a tie to keep the newer record. Note that the record info
              // iterator will return records with incremental doc ids.
              if (segment == currentSegment) {
                if (comparisonResult >= 0) {
                  validDocIds.replace(currentRecordLocation.getDocId(), recordInfo.getDocId());
                  RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                      new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
                  return RecordLocationSerDe.serialize(newRecordLocationWithSegmentId);
                } else {
                  return value;
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
                  return RecordLocationSerDe.serialize(newRecordLocationWithSegmentId);
                } else {
                  return value;
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
                return RecordLocationSerDe.serialize(newRecordLocationWithSegmentId);
              } else {
                return value;
              }
            } else {
              // New primary key
              validDocIds.add(recordInfo.getDocId());
              RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                  new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
              return RecordLocationSerDe.serialize(newRecordLocationWithSegmentId);
            }
          });
    }
    // Update metrics
    if(_serverMetrics != null ) {
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
          _primaryKeyToRecordLocationMap.size());
    }
  }

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment.
   */
  @Override
  public void addRecord(IndexSegment segment, RecordInfo recordInfo) {
    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    int segmentId = getSegmentId(segment);
    _primaryKeyToRecordLocationMap.compute(recordInfo.getPrimaryKey().asBytes(),
        (primaryKey, value) -> {
          if (value != null) {
            RecordLocationWithSegmentId currentRecordLocation = RecordLocationSerDe.deserialize(value);
            // Existing primary key

            // Update the record location when the new comparison value is greater than or equal to the current value.
            // Update the record location when there is a tie to keep the newer record.
            if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
              IndexSegment currentSegment = (IndexSegment) _segmentIdToSegmentMap.get(currentRecordLocation.getSegmentId());
              int currentDocId = currentRecordLocation.getDocId();
              if (segment == currentSegment) {
                validDocIds.replace(currentDocId, recordInfo.getDocId());
              } else {
                Objects.requireNonNull(currentSegment.getValidDocIds()).remove(currentDocId);
                validDocIds.add(recordInfo.getDocId());
              }
              RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                  new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
              return RecordLocationSerDe.serialize(newRecordLocationWithSegmentId);
            } else {
              return value;
            }
          } else {
            // New primary key
            validDocIds.add(recordInfo.getDocId());
            RecordLocationWithSegmentId newRecordLocationWithSegmentId =
                new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
            return RecordLocationSerDe.serialize(newRecordLocationWithSegmentId);
          }
        });
    // Update metrics
    if(_serverMetrics != null ) {
      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
          _primaryKeyToRecordLocationMap.size());
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

  /**
   * Returns the merged record when partial-upsert is enabled.
   */
  @Override
  public GenericRow updateRecord(GenericRow record, RecordInfo recordInfo) {
    throw new NotImplementedException();
//    // Directly return the record when partial-upsert is not enabled
//    if (_partialUpsertHandler == null) {
//      return record;
//    }
//
//    // Ensure all previous records are loaded before inserting new records
//    while (!_partialUpsertHandler.isAllSegmentsLoaded()) {
//      LOGGER.info("Sleeping 1 second waiting for all segments loaded for partial-upsert table: {}", _tableNameWithType);
//      try {
//        //noinspection BusyWait
//        Thread.sleep(1000L);
//      } catch (InterruptedException e) {
//        throw new RuntimeException(e);
//      }
//    }
//
//    RecordLocationWithSegmentId currentRecordLocation =
//        _primaryKeyToRecordLocationMap.get(recordInfo.getPrimaryKey());
//    if (currentRecordLocation != null) {
//      // Existing primary key
//      if (recordInfo.getComparisonValue().compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
//        _reuse.clear();
//        GenericRow previousRecord =
//            currentRecordLocation.getSegment().getRecord(currentRecordLocation.getDocId(), _reuse);
//        return _partialUpsertHandler.merge(previousRecord, record);
//      } else {
//        LOGGER.warn(
//            "Got late event for partial-upsert: {} (current comparison value: {}, record comparison value: {}), "
//                + "skipping updating the record", record, currentRecordLocation.getComparisonValue(),
//            recordInfo.getComparisonValue());
//        return record;
//      }
//    } else {
//      // New primary key
//      return record;
//    }
  }

  /**
   * Removes the upsert metadata for the given immutable segment. No need to remove the upsert metadata for the
   * consuming segment because it should be replaced by the committed segment.
   */
  @Override
  public void removeSegment(IndexSegment segment) {
//    String segmentName = segment.getSegmentName();
//    LOGGER.info("Removing upsert metadata for segment: {}", segmentName);
//
//    if (!Objects.requireNonNull(segment.getValidDocIds()).getMutableRoaringBitmap().isEmpty()) {
//      // Remove all the record locations that point to the removed segment
//      _primaryKeyToRecordLocationMap.forEach((primaryKey, recordLocation) -> {
//        if (recordLocation.getSegment() == segment) {
//          // Check and remove to prevent removing the key that is just updated
//          _primaryKeyToRecordLocationMap.remove(primaryKey, recordLocation);
//        }
//      });
//    }
//    // Update metrics
//    if(_serverMetrics != null ) {
//      _serverMetrics.setValueOfPartitionGauge(_tableNameWithType, _partitionId, ServerGauge.UPSERT_PRIMARY_KEYS_COUNT,
//          _primaryKeyToRecordLocationMap.size());
//    }
  }

  @Override
  public void close() {
//    System.out.println("----CHRONICLE_MAP SEGMENT STATS----");
//    for(ChronicleMap.SegmentStats segmentStats: _primaryKeyToRecordLocationMap.segmentStats()) {
//      System.out.println(segmentStats.toString());
//    }
    System.out.println("PERCENT FREE SPACE: " +  _primaryKeyToRecordLocationMap.percentageFreeSpace());
    System.out.println("TOTAL KEYS CHRONICLE MAP: " + _primaryKeyToRecordLocationMap.size());
    _primaryKeyToRecordLocationMap.close();
  }
}
