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
package org.apache.pinot.query.planner.spi.stats;

import java.util.List;
import java.util.OptionalLong;
import javax.annotation.Nullable;


/// Planner-facing read API for table and column statistics.
///
/// Implementations MUST be cheap — this interface is called multiple times per query on the
/// planning hot path. Implementations MUST be thread-safe; they expose the merged logical view
/// of a table.
///
/// Table name semantics:
/// - A raw table name (no type suffix) requests the logical hybrid view: implementations must
///   merge OFFLINE and REALTIME statistics at the time boundary without double-counting rows.
/// - A name with a type suffix (e.g. `myTable_OFFLINE`) requests the physical view for
///   that specific table type.
///
/// Thread-safety: implementations must be thread-safe.
public interface PinotStatisticsProvider {

  /// Returns aggregate statistics for the given table, or `null` if no statistics are available.
  ///
  /// @param tableName raw table name (logical hybrid view) or a name with type suffix (physical
  ///                  view)
  @Nullable
  TableStatistics getTableStatistics(String tableName);

  /// Returns per-column statistics for the given table and column, or `null` if no statistics
  /// are available.
  ///
  /// @param tableName  raw table name (logical hybrid view) or a name with type suffix (physical
  ///                   view)
  /// @param columnName name of the column
  @Nullable
  ColumnStatistics getColumnStatistics(String tableName, String columnName);

  /// Returns an estimate of the number of rows whose time column falls in the half-open interval
  /// `[startMs, endMs)`, or an empty optional if the estimate cannot be produced.
  ///
  /// Used for time-predicate selectivity estimation. Implementations may return an empty
  /// optional when time-range metadata is not available.
  ///
  /// @param tableName raw table name or name with type suffix
  /// @param startMs   start of the time range, inclusive, in epoch milliseconds
  /// @param endMs     end of the time range, exclusive, in epoch milliseconds
  default OptionalLong estimateRowsInTimeRange(String tableName, long startMs, long endMs) {
    return OptionalLong.empty();
  }

  /// Returns per-segment statistics for the (non-consuming) segments of the given column whose
  /// stored `[min, max]` range overlaps the numeric predicate window, capped at `limit` rows.
  ///
  /// This is the segment-aware (query-aware) entry point: it pushes the numeric range prune into
  /// the store so that whole segments that cannot contain a matching value are excluded before the
  /// planner composes the surviving-segment set. Implementations MUST treat the prune as an
  /// over-approximation (overlap, not containment) and MUST NOT use the result for anything that
  /// affects correctness — it is purely an estimation aid.
  ///
  /// Returning fewer than `limit` rows means the prune was exhaustive; returning exactly `limit`
  /// rows signals the caller that the survivor set may be larger (the caller then degrades to the
  /// aggregated estimate). An empty list means no segment-aware data is available, in which case
  /// the planner falls back to [#getColumnStatistics].
  ///
  /// @param tableName  raw table name (logical hybrid view: union over OFFLINE + REALTIME) or a
  ///                   name with a type suffix (physical view)
  /// @param columnName name of the column
  /// @param predicate  numeric overlap window to prune by
  /// @param limit      maximum number of surviving segments to return
  default List<SegmentColumnStat> getSurvivingSegmentColumnStats(String tableName,
      String columnName, ColumnPredicate predicate, int limit) {
    return List.of();
  }

  /// Returns per-segment statistics for all (non-consuming) segments of the given column, capped at
  /// `limit` rows. Used by the segment-aware estimator for string columns, where the prune must be
  /// performed in Java (to honor the server's string comparator rather than the store's byte-order
  /// comparator).
  ///
  /// An empty list means no segment-aware data is available.
  ///
  /// @param tableName  raw table name (logical hybrid view) or a name with a type suffix
  /// @param columnName name of the column
  /// @param limit      maximum number of segments to return
  default List<SegmentColumnStat> getSegmentColumnStats(String tableName, String columnName,
      int limit) {
    return List.of();
  }
}
