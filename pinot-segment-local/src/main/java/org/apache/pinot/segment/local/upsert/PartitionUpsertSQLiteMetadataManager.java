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
import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
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
import org.rocksdb.RocksDBException;
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
public class PartitionUpsertSQLiteMetadataManager implements IPartitionUpsertMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertSQLiteMetadataManager.class);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final PartialUpsertHandler _partialUpsertHandler;
  private final HashFunction _hashFunction;

  // TODO(upsert): consider an off-heap KV store to persist this mapping to improve the recovery speed.
  @VisibleForTesting
  private final ConcurrentHashMap<Object, Integer> _segmentToSegmentIdMap = new ConcurrentHashMap<>();
  //need to create a second reverse lookup hashmap, any way to avoid it?
  private final ConcurrentHashMap<Integer, Object> _segmentIdToSegmentMap = new ConcurrentHashMap<>();
  private final AtomicInteger _segmentId = new AtomicInteger();
  Connection _sqlDB = null;

  // Reused for reading previous record during partial upsert
  private final GenericRow _reuse = new GenericRow();

  public PartitionUpsertSQLiteMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      @Nullable PartialUpsertHandler partialUpsertHandler, HashFunction hashFunction) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _partialUpsertHandler = partialUpsertHandler;
    _hashFunction = hashFunction;

    try {
      Class.forName("org.sqlite.JDBC");
      _sqlDB = DriverManager.getConnection("jdbc:sqlite:" + PartitionUpsertSQLiteMetadataManager.class.getSimpleName() + System.currentTimeMillis() + ".db");
      Statement statement = _sqlDB.createStatement();
      statement.executeUpdate("CREATE TABLE UPSERT_METADATA(\n" + "   UPSERT_KEY VARCHAR(200) PRIMARY KEY  NOT NULL,\n"
          + "   SEGMENT_ID     INT    ,\n" + "   DOC_ID         INT    ,\n" + "   COMPARISON_VALUE  INT\n" + ");");
      statement.close();
    } catch ( Exception e ) {
      LOGGER.error("Could not open database connection", e);
    }

  }

  @Override
  public void addSegment(IndexSegment segment, Iterator<RecordInfo> recordInfoIterator) {
    String segmentName = segment.getSegmentName();
    int segmentId = getSegmentId(segment);

    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      try {
        RecordLocationWithSegmentId currentRecordLocation = getRecordLocation(recordInfo.getPrimaryKey());

        if (currentRecordLocation != null) {
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
              updateRecordLocation(recordInfo.getPrimaryKey(), newRecordLocationWithSegmentId);
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
              updateRecordLocation(recordInfo.getPrimaryKey(), newRecordLocationWithSegmentId);
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
            updateRecordLocation(recordInfo.getPrimaryKey(), newRecordLocationWithSegmentId);
          } else {
            //
          }
        } else {
          // New primary key
          validDocIds.add(recordInfo.getDocId());
          RecordLocationWithSegmentId newRecordLocationWithSegmentId =
              new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
          updateRecordLocation(recordInfo.getPrimaryKey(), newRecordLocationWithSegmentId);
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

    try {
      RecordLocationWithSegmentId currentRecordLocation = getRecordLocation(recordInfo.getPrimaryKey());
        if (currentRecordLocation != null) {

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
            updateRecordLocation(recordInfo.getPrimaryKey(), newRecordLocationWithSegmentId);
          } else {
            //txn.put(key, RecordLocationSerDe.serialize(currentRecordLocation));
          }
        } else {
          validDocIds.add(recordInfo.getDocId());
          int segmentId = getSegmentId(segment);
          RecordLocationWithSegmentId newRecordLocationWithSegmentId =
              new RecordLocationWithSegmentId(segmentId, recordInfo.getDocId(), recordInfo.getComparisonValue());
          updateRecordLocation(recordInfo.getPrimaryKey(), newRecordLocationWithSegmentId);
        }
    } catch (Exception sqliteException) {
      // log error
      sqliteException.printStackTrace();
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
  }

  private RecordLocationWithSegmentId getRecordLocation(PrimaryKey primaryKey) {
    try {
      PreparedStatement statement = _sqlDB.prepareStatement("SELECT SEGMENT_ID, DOC_ID, COMPARISON_VALUE FROM UPSERT_METADATA WHERE UPSERT_KEY=?");
      statement.setString(1, primaryKey.toString());
      ResultSet resultSet = statement.executeQuery();
      RecordLocationWithSegmentId recordLocationWithSegmentId = null;
      if(resultSet.next()) {
        int segmentId = resultSet.getInt(1);
        int docId = resultSet.getInt(2);
        long comparisonValue = resultSet.getLong(3);
        recordLocationWithSegmentId = new RecordLocationWithSegmentId(segmentId, docId, comparisonValue);
      }
      statement.close();
      resultSet.close();
      return recordLocationWithSegmentId;
    } catch (Exception e) {
      LOGGER.warn("Could not execute query", e);
    }
    return null;
  }

  private void updateRecordLocation(PrimaryKey primaryKey, RecordLocationWithSegmentId recordLocationWithSegmentId) {
    try{
      PreparedStatement statement = _sqlDB.prepareStatement("INSERT INTO UPSERT_METADATA(SEGMENT_ID,"
          + "DOC_ID,COMPARISON_VALUE, UPSERT_KEY) VALUES(?, ?, ?, ?)\n"
          + "  ON CONFLICT(UPSERT_KEY) DO UPDATE SET SEGMENT_ID=excluded.SEGMENT_ID, DOC_ID=excluded.DOC_ID, "
          + "COMPARISON_VALUE=excluded.COMPARISON_VALUE;");
      statement.setInt(1, recordLocationWithSegmentId.getSegmentId());
      statement.setInt(2, recordLocationWithSegmentId.getDocId());
      statement.setLong(3, recordLocationWithSegmentId.getComparisonValue());
      statement.setString(4, primaryKey.toString());
      statement.executeUpdate();
      statement.close();
    } catch (Exception e) {
      LOGGER.warn("Could not execute update", e);
    }
  }

  /**
   * Removes the upsert metadata for the given immutable segment. No need to remove the upsert metadata for the
   * consuming segment because it should be replaced by the committed segment.
   */
  @Override
  public void removeSegment(IndexSegment segment) {
    throw new NotImplementedException();
  }

  @Override
  public void close() {
    try {
      _sqlDB.close();
    } catch (Exception e) {
      LOGGER.error("Error occurred while closing database", e);
    }
  }
}
