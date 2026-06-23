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


/// A numeric column predicate expressed as the closed value window `[lo, hi]` that a segment's
/// `[min, max]` range must overlap to possibly contain a matching value.
///
/// This is the input to [PinotStatisticsProvider#survivingSegmentStats]: the store keeps a segment
/// only when `segment.min <= hi AND segment.max >= lo`. The mapping from a SQL comparison to this
/// window is:
/// - `col = v`   → `[v, v]`
/// - `col > v`   → `[v, +inf)`  (represented as `lo = v`, `hi = +inf`)
/// - `col >= v`  → `[v, +inf)`
/// - `col < v`   → `(-inf, v]`  (represented as `lo = -inf`, `hi = v`)
/// - `col <= v`  → `(-inf, v]`
/// - `v1 <= col < v2` → `[v1, v2]`
///
/// Because this window is used only for *pruning* (an over-approximation — a segment whose range
/// overlaps may still not contain a match), inclusive-vs-exclusive endpoints are treated
/// identically; the within-segment fraction handles the residual.
///
/// String predicates are NOT expressed through this type; the estimator fetches all per-segment
/// rows via [PinotStatisticsProvider#segmentColumnStats] and prunes them in Java to honor the
/// server's string comparator.
///
/// Thread-safety: immutable; safe for concurrent access.
public class ColumnPredicate {
  private final double _lo;
  private final double _hi;

  private ColumnPredicate(double lo, double hi) {
    _lo = lo;
    _hi = hi;
  }

  /// Returns a predicate matching segments that may contain the value `v` (`col = v`).
  public static ColumnPredicate equalTo(double v) {
    return new ColumnPredicate(v, v);
  }

  /// Returns a predicate for the closed window `[lo, hi]`. Use [Double#NEGATIVE_INFINITY] /
  /// [Double#POSITIVE_INFINITY] for open sides (e.g. `col >= lo` is `range(lo, +inf)`).
  public static ColumnPredicate range(double lo, double hi) {
    return new ColumnPredicate(lo, hi);
  }

  /// Lower bound of the window (inclusive); [Double#NEGATIVE_INFINITY] when unbounded below.
  public double getLo() {
    return _lo;
  }

  /// Upper bound of the window (inclusive); [Double#POSITIVE_INFINITY] when unbounded above.
  public double getHi() {
    return _hi;
  }
}
