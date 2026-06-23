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

import javax.annotation.Nullable;


/// Immutable per-segment statistics for a single column, used by the segment-aware (query-aware)
/// selectivity estimator.
///
/// Unlike [ColumnStatistics] — which collapses all segments of a table into one aggregated view —
/// these objects preserve the per-segment granularity needed to prune whole segments that cannot
/// satisfy a predicate, and to sum the surviving segments' restricted row contributions.
///
/// Min/max values are carried as strings exactly as persisted in the broker stats store; the
/// estimator parses them to numbers for numeric columns and compares them lexically for string
/// columns. Unknown numeric fields are represented by `-1`.
///
/// Thread-safety: immutable; safe for concurrent access.
public class SegmentColumnStat {
  @Nullable
  private final String _segment;
  private final long _segmentId;
  private final long _totalDocs;
  private final long _ndv;
  private final StatConfidence _ndvConfidence;
  @Nullable
  private final String _minValue;
  @Nullable
  private final String _maxValue;
  private final boolean _minTrusted;

  /// Constructs a per-segment column stat from a precomputed segment id.
  ///
  /// The store persists [#getSegmentId] as an INTEGER column and reads it back directly, so on the
  /// planning read path the segment name is never materialized — `segment` is `null` there. Callers
  /// that have the name in hand (tests, write side) use [#fromName].
  ///
  /// @param segmentId     stable 64-bit segment id (see [#hashSegmentName])
  /// @param segment       segment name, or `null` when only the id is available
  /// @param totalDocs     total document count of the segment (all columns), or `-1` if unknown
  /// @param ndv           number of distinct values of this column in this segment, or `-1`
  /// @param ndvConfidence confidence of the NDV value
  /// @param minValue      string-serialized minimum value, or `null` if unknown
  /// @param maxValue      string-serialized maximum value, or `null` if unknown
  /// @param minTrusted    `false` when the minimum may be polluted by the numeric null-sentinel
  public SegmentColumnStat(long segmentId, @Nullable String segment, long totalDocs, long ndv,
      StatConfidence ndvConfidence, @Nullable String minValue, @Nullable String maxValue,
      boolean minTrusted) {
    _segment = segment;
    _segmentId = segmentId;
    _totalDocs = totalDocs;
    _ndv = ndv;
    _ndvConfidence = ndvConfidence;
    _minValue = minValue;
    _maxValue = maxValue;
    _minTrusted = minTrusted;
  }

  /// Constructs a per-segment column stat from a segment name, deriving the id via
  /// [#hashSegmentName]. Convenience for callers (and tests) that hold the name.
  public static SegmentColumnStat fromName(String segment, long totalDocs, long ndv,
      StatConfidence ndvConfidence, @Nullable String minValue, @Nullable String maxValue,
      boolean minTrusted) {
    return new SegmentColumnStat(hashSegmentName(segment), segment, totalDocs, ndv, ndvConfidence,
        minValue, maxValue, minTrusted);
  }

  /// Returns the segment name, or `null` when this stat was read by id only.
  @Nullable
  public String getSegment() {
    return _segment;
  }

  /// Returns a stable 64-bit identifier for the segment.
  ///
  /// The segment-aware estimator keys its surviving-segment sets/maps by this `long` instead of the
  /// name, so set intersection/union across the predicate tree use O(1) primitive equality and hold
  /// no per-segment `String`s. The store persists this id, so the name need never be read on the
  /// planning path. Because the structure is purely an estimation aid, the rare 64-bit hash
  /// collision is harmless — it can only fold two distinct segments together, adding a small false
  /// positive (a slight over-estimate), never a wrong result.
  public long getSegmentId() {
    return _segmentId;
  }

  /// 64-bit FNV-1a hash of a segment name, used as the persisted, process-stable segment id (unlike
  /// `String.hashCode`, which is only 32-bit and thus far more collision-prone over many segments).
  public static long hashSegmentName(String s) {
    long h = 0xcbf29ce484222325L;
    for (int i = 0; i < s.length(); i++) {
      h ^= s.charAt(i);
      h *= 0x100000001b3L;
    }
    return h;
  }

  /// Returns the total document count of the segment, or `-1` if unknown.
  public long getTotalDocs() {
    return _totalDocs;
  }

  /// Returns the number of distinct values of this column in this segment, or `-1` if unknown.
  public long getNdv() {
    return _ndv;
  }

  /// Returns the confidence of the [#getNdv()] value.
  public StatConfidence getNdvConfidence() {
    return _ndvConfidence;
  }

  /// Returns the string-serialized minimum value for this column in this segment, or `null`.
  @Nullable
  public String getMinValue() {
    return _minValue;
  }

  /// Returns the string-serialized maximum value for this column in this segment, or `null`.
  @Nullable
  public String getMaxValue() {
    return _maxValue;
  }

  /// Returns `false` when the minimum value may be polluted by the numeric null-sentinel default.
  /// When `false`, the lower bound must not be used to exclude this segment from an estimate.
  public boolean isMinTrusted() {
    return _minTrusted;
  }
}
