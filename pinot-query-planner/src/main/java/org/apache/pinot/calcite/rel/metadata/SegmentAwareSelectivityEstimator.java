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
package org.apache.pinot.calcite.rel.metadata;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import javax.annotation.Nullable;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Sarg;
import org.apache.pinot.query.planner.spi.stats.ColumnPredicate;
import org.apache.pinot.query.planner.spi.stats.PinotStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.SegmentColumnStat;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.spi.data.FieldSpec;


/// Segment-aware (query-aware) selectivity estimator.
///
/// Where [PinotRelMdSelectivity]'s aggregated path collapses all segments of a table into one
/// `[min, max]`/`ndv` view before estimating, this estimator preserves per-segment granularity: it
/// prunes whole segments whose stored `[min, max]` cannot satisfy a leaf predicate, composes the
/// surviving-segment sets over the boolean tree (`AND` = intersect, `OR` = union), and sums the
/// surviving segments' restricted row contributions. This is tighter in two ways — it stops
/// spreading probability mass over segments a predicate provably excludes, and it makes the
/// cross-segment component of a multi-predicate `AND` exact (a segment that fails any conjunct
/// contributes exactly 0), leaving only the within-segment residual to the independence assumption.
///
/// ### Estimate only
/// The surviving set is an over-approximation (range overlap, not containment) and is used purely
/// to estimate rows; it never drives scan pruning, so a loose or stale result can only yield a
/// worse plan, never wrong results. When stats are missing, low-confidence, or the survivor set
/// exceeds the cost guard, the estimator returns [OptionalDouble#empty] and the caller falls back
/// to the aggregated estimate.
///
/// ### Lifecycle / threading
/// One instance is [installed][#install] per query into a [ThreadLocal] by
/// [org.apache.pinot.query.QueryEnvironment] before planning and [cleared][#clear] in a `finally`.
/// MSE planning runs single-threaded on the request thread, so the per-query memo needs no
/// synchronization. The estimator is consulted by the (globally-shared, stateless) metadata handler
/// via [#current]; when no estimator is installed (feature off), the handler uses only the
/// aggregated path.
public final class SegmentAwareSelectivityEstimator {

  /// Cost guard: maximum number of surviving segments for which the per-segment sum is performed.
  /// Beyond this a leaf/branch is treated as non-pruning and the aggregated estimate is used. At the
  /// hundreds-of-segments reality this is never hit by a selective predicate; it bounds the
  /// non-selective case (e.g. `col IS NOT NULL`) and the Java-side transfer.
  static final int MAX_SURVIVORS = 1024;

  /// Maximum number of rows fetched for the string (Java-pruned) path before giving up; if a column
  /// has more segments than this the leaf is treated as non-pruning (we cannot be sure we saw all).
  static final int MAX_STRING_FETCH = 4096;

  /// Within-segment fraction used when a surviving segment has no reliable distribution information
  /// (untrusted min with a non-positive max, or a string range). Mirrors
  /// [PinotRelMdSelectivity]'s `FALLBACK_RANGE_SELECTIVITY`.
  static final double UNTRUSTED_FALLBACK_FRACTION = 0.25;

  private static final ThreadLocal<SegmentAwareSelectivityEstimator> CURRENT = new ThreadLocal<>();

  /// Memo keyed by the predicate `RexNode` identity → its non-time selectivity (empty = no
  /// segment-aware estimate; caller uses the aggregated path).
  private final Map<RexNode, OptionalDouble> _memo = new IdentityHashMap<>();

  private SegmentAwareSelectivityEstimator() {
  }

  /// Installs a fresh per-query estimator on the current thread.
  public static void install() {
    CURRENT.set(new SegmentAwareSelectivityEstimator());
  }

  /// Removes the estimator from the current thread. Safe to call when none is installed.
  public static void clear() {
    CURRENT.remove();
  }

  /// Returns the estimator installed on the current thread, or `null` if the feature is off.
  @Nullable
  public static SegmentAwareSelectivityEstimator current() {
    return CURRENT.get();
  }

  /// Estimates the combined selectivity of the `AND` of `nonTimeConjuncts` using per-segment stats.
  ///
  /// @param memoKey               the predicate node the conjuncts were derived from (memo key)
  /// @param nonTimeConjuncts      the conjuncts to estimate (implicitly AND-ed)
  /// @param tableName             table name (raw logical or suffixed physical)
  /// @param provider              statistics provider
  /// @param tableRowCount         positive total row count used as the selectivity denominator
  /// @param colNameResolver       maps a scan input-ref to its column name (`null` if unresolvable)
  /// @param dataTypeResolver      maps a column name to its stored data type (`null` if unknown)
  /// @param aggregatedSelectivity aggregated per-conjunct selectivity, used for non-pruning parts
  /// @return the segment-aware selectivity in `[0, 1]`, or empty to fall back to the aggregated path
  public OptionalDouble estimate(RexNode memoKey, List<RexNode> nonTimeConjuncts, String tableName,
      PinotStatisticsProvider provider, double tableRowCount,
      Function<RexInputRef, String> colNameResolver,
      Function<String, FieldSpec.DataType> dataTypeResolver,
      ToDoubleFunction<RexNode> aggregatedSelectivity) {
    OptionalDouble cached = _memo.get(memoKey);
    if (cached != null) {
      return cached;
    }
    OptionalDouble result;
    try {
      result = doEstimate(nonTimeConjuncts, tableName, provider, tableRowCount, colNameResolver,
          dataTypeResolver, aggregatedSelectivity);
    } catch (RuntimeException e) {
      // Estimation must never fail planning; degrade to the aggregated path.
      result = OptionalDouble.empty();
    }
    _memo.put(memoKey, result);
    return result;
  }

  private OptionalDouble doEstimate(List<RexNode> conjuncts, String tableName,
      PinotStatisticsProvider provider, double tableRowCount,
      Function<RexInputRef, String> colNameResolver,
      Function<String, FieldSpec.DataType> dataTypeResolver,
      ToDoubleFunction<RexNode> aggregatedSelectivity) {
    EstimationContext ctx =
        new EstimationContext(tableName, provider, colNameResolver, dataTypeResolver,
            aggregatedSelectivity);

    List<Long2DoubleOpenHashMap> prunedMatches = new ArrayList<>();
    double scalarSelectivity = 1.0;
    for (RexNode conjunct : conjuncts) {
      ConjunctEstimate ce = process(conjunct, ctx);
      if (ce._pruned) {
        prunedMatches.add(ce._matchRows);
      } else {
        scalarSelectivity *= ce._scalarSelectivity;
      }
    }
    if (prunedMatches.isEmpty()) {
      // No conjunct could be pruned segment-wise — the aggregated path is just as good.
      return OptionalDouble.empty();
    }

    // Top-level AND: matched rows per surviving segment, summed across the table.
    Long2DoubleOpenHashMap combined = andCombine(prunedMatches, scalarSelectivity, ctx);
    double estRows = 0.0;
    for (double matchRows : combined.values()) {
      estRows += matchRows;
    }
    return OptionalDouble.of(clamp(estRows / tableRowCount));
  }

  // --------------------------------------------------------------------------
  // RexNode recursion
  // --------------------------------------------------------------------------

  private ConjunctEstimate process(RexNode node, EstimationContext ctx) {
    SqlKind kind = node.getKind();
    switch (kind) {
      case AND:
        return processAnd((RexCall) node, ctx);
      case OR:
        return processOr((RexCall) node, ctx);
      case EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        return processComparison((RexCall) node, ctx);
      case SEARCH:
        return processSearch((RexCall) node, ctx);
      default:
        return ConjunctEstimate.scalar(ctx.aggregated(node));
    }
  }

  /// Nested `AND`: intersect pruned children's surviving segments; non-pruning children fold into a
  /// uniform scalar applied to every surviving segment.
  private ConjunctEstimate processAnd(RexCall and, EstimationContext ctx) {
    List<Long2DoubleOpenHashMap> prunedMatches = new ArrayList<>();
    double scalar = 1.0;
    for (RexNode operand : and.getOperands()) {
      ConjunctEstimate ce = process(operand, ctx);
      if (ce._pruned) {
        prunedMatches.add(ce._matchRows);
      } else {
        scalar *= ce._scalarSelectivity;
      }
    }
    if (prunedMatches.isEmpty()) {
      return ConjunctEstimate.scalar(ctx.aggregated(and));
    }
    return ConjunctEstimate.pruned(andCombine(prunedMatches, scalar, ctx));
  }

  /// `OR`: prunable only when every disjunct is prunable (an unknown disjunct could match any
  /// segment). Survivors = union; per-segment matched rows combined by inclusion-exclusion.
  private ConjunctEstimate processOr(RexCall or, EstimationContext ctx) {
    List<Long2DoubleOpenHashMap> childMatches = new ArrayList<>();
    for (RexNode operand : or.getOperands()) {
      ConjunctEstimate ce = process(operand, ctx);
      if (!ce._pruned) {
        return ConjunctEstimate.scalar(ctx.aggregated(or));
      }
      childMatches.add(ce._matchRows);
    }
    LongSet union = unionKeys(childMatches);
    if (union.size() > MAX_SURVIVORS) {
      return ConjunctEstimate.scalar(ctx.aggregated(or));
    }
    Long2DoubleOpenHashMap result = new Long2DoubleOpenHashMap();
    for (LongIterator it = union.iterator(); it.hasNext();) {
      long seg = it.nextLong();
      long td = ctx._totalDocs.get(seg);
      if (td <= 0) {
        continue;
      }
      // P(A∨B) = 1 - Π(1 - P(child)); a child missing this segment contributes P=0 (factor 1).
      double complement = 1.0;
      for (Long2DoubleOpenHashMap child : childMatches) {
        double f = child.get(seg) / td;   // absent → 0.0 (the map's default), so factor 1
        complement *= (1.0 - f);
      }
      double matchRows = td * (1.0 - complement);
      if (matchRows > 0) {
        result.put(seg, matchRows);
      }
    }
    return ConjunctEstimate.pruned(result);
  }

  /// Combines pruned children under `AND` over their intersected surviving segments, returning
  /// matched rows per segment. For a segment surviving all `k` children, the joint count under
  /// within-segment independence is `scalar · Π matchRows_i / totalDocs^(k-1)` (each child's count
  /// is `totalDocs · fraction_i`, and the joint is `totalDocs · scalar · Π fraction_i`).
  private static Long2DoubleOpenHashMap andCombine(List<Long2DoubleOpenHashMap> children,
      double scalar, EstimationContext ctx) {
    LongSet survivors = intersectKeys(children);
    Long2DoubleOpenHashMap result = new Long2DoubleOpenHashMap();
    for (LongIterator it = survivors.iterator(); it.hasNext();) {
      long seg = it.nextLong();
      long td = ctx._totalDocs.get(seg);
      if (td <= 0) {
        continue;
      }
      double acc = scalar;
      for (Long2DoubleOpenHashMap child : children) {
        acc *= child.get(seg);
      }
      // Divide out the over-counted base: k children multiplied k counts, each carrying one td.
      for (int i = 1; i < children.size(); i++) {
        acc /= td;
      }
      double matchRows = Math.min(acc, td);
      if (matchRows > 0) {
        result.put(seg, matchRows);
      }
    }
    return result;
  }

  private ConjunctEstimate processComparison(RexCall call, EstimationContext ctx) {
    if (call.getOperands().size() != 2) {
      return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    RexNode left = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);
    SqlKind kind = call.getKind();
    RexInputRef ref;
    RexLiteral literal;
    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      ref = (RexInputRef) left;
      literal = (RexLiteral) right;
    } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
      ref = (RexInputRef) right;
      literal = (RexLiteral) left;
      kind = flip(kind);
    } else {
      return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    String colName = ctx._colNameResolver.apply(ref);
    if (colName == null) {
      return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    FieldSpec.DataType dataType = ctx._dataTypeResolver.apply(colName);
    if (isNumeric(dataType)) {
      return numericLeaf(call, ctx, colName, kind, literal);
    }
    if (dataType == FieldSpec.DataType.STRING) {
      return stringLeaf(call, ctx, colName, kind, literal);
    }
    return ConjunctEstimate.scalar(ctx.aggregated(call));
  }

  /// Handles `SEARCH(col, Sarg)` — Calcite's rewrite of range / IN predicates. Only a single
  /// numeric range (or single point) is pruned segment-wise; anything else is non-pruning.
  private ConjunctEstimate processSearch(RexCall call, EstimationContext ctx) {
    if (call.getOperands().size() != 2 || !(call.getOperands().get(0) instanceof RexInputRef)
        || !(call.getOperands().get(1) instanceof RexLiteral)) {
      return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    RexInputRef ref = (RexInputRef) call.getOperands().get(0);
    String colName = ctx._colNameResolver.apply(ref);
    if (colName == null || !isNumeric(ctx._dataTypeResolver.apply(colName))) {
      return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    Sarg<?> sarg = ((RexLiteral) call.getOperands().get(1)).getValueAs(Sarg.class);
    if (sarg == null || sarg.rangeSet == null || sarg.rangeSet.asRanges().size() != 1) {
      return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    Range<?> range = sarg.rangeSet.asRanges().iterator().next();
    double lo = range.hasLowerBound() ? toDouble(range.lowerEndpoint()) : Double.NEGATIVE_INFINITY;
    double hi = range.hasUpperBound() ? toDouble(range.upperEndpoint()) : Double.POSITIVE_INFINITY;
    if (Double.isNaN(lo) || Double.isNaN(hi)) {
      return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    // Inclusive lo/hi treatment for both pruning and within-segment fraction (the ±1 at the
    // boundary is negligible at estimation scale).
    boolean loInclusive = !range.hasLowerBound() || range.lowerBoundType() == BoundType.CLOSED;
    boolean hiInclusive = !range.hasUpperBound() || range.upperBoundType() == BoundType.CLOSED;
    return numericRangeWindow(ctx, colName, call, lo, hi, loInclusive, hiInclusive);
  }

  // --------------------------------------------------------------------------
  // Numeric leaf
  // --------------------------------------------------------------------------

  private ConjunctEstimate numericLeaf(RexCall call, EstimationContext ctx, String colName,
      SqlKind kind, RexLiteral literal) {
    Double v = literalToDouble(literal);
    if (v == null) {
      return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    if (kind == SqlKind.EQUALS) {
      List<SegmentColumnStat> survivors =
          ctx._provider.getSurvivingSegmentColumnStats(ctx._tableName, colName,
              ColumnPredicate.equalTo(v), MAX_SURVIVORS + 1);
      if (survivors.isEmpty() || survivors.size() > MAX_SURVIVORS) {
        return ConjunctEstimate.scalar(ctx.aggregated(call));
      }
      Long2DoubleOpenHashMap matchRows = new Long2DoubleOpenHashMap();
      for (SegmentColumnStat s : survivors) {
        if (!usable(s) || s.getNdv() < 1) {
          return ConjunctEstimate.scalar(ctx.aggregated(call));
        }
        long td = s.getTotalDocs();
        if (td <= 0) {
          continue;
        }
        ctx.recordTotalDocs(s);
        // matched rows = totalDocs * (1 / ndv)
        matchRows.put(s.getSegmentId(), td / (double) s.getNdv());
      }
      return matchRows.isEmpty() ? ConjunctEstimate.scalar(ctx.aggregated(call))
          : ConjunctEstimate.pruned(matchRows);
    }
    // Range comparison.
    double lo;
    double hi;
    switch (kind) {
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        lo = v;
        hi = Double.POSITIVE_INFINITY;
        break;
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        lo = Double.NEGATIVE_INFINITY;
        hi = v;
        break;
      default:
        return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    boolean loInclusive = kind != SqlKind.GREATER_THAN;
    boolean hiInclusive = kind != SqlKind.LESS_THAN;
    return numericRangeWindow(ctx, colName, call, lo, hi, loInclusive, hiInclusive);
  }

  /// Fetches segments overlapping `[lo, hi]` and computes each survivor's within-segment fraction.
  private ConjunctEstimate numericRangeWindow(EstimationContext ctx, String colName, RexNode node,
      double lo, double hi, boolean loInclusive, boolean hiInclusive) {
    List<SegmentColumnStat> survivors = ctx._provider.getSurvivingSegmentColumnStats(ctx._tableName,
        colName, ColumnPredicate.range(lo, hi), MAX_SURVIVORS + 1);
    if (survivors.isEmpty() || survivors.size() > MAX_SURVIVORS) {
      return ConjunctEstimate.scalar(ctx.aggregated(node));
    }
    Long2DoubleOpenHashMap matchRows = new Long2DoubleOpenHashMap();
    for (SegmentColumnStat s : survivors) {
      if (!usable(s)) {
        return ConjunctEstimate.scalar(ctx.aggregated(node));
      }
      Double segMin = parseDouble(s.getMinValue());
      Double segMax = parseDouble(s.getMaxValue());
      if (segMin == null || segMax == null) {
        return ConjunctEstimate.scalar(ctx.aggregated(node));
      }
      long td = s.getTotalDocs();
      if (td <= 0) {
        continue;
      }
      ctx.recordTotalDocs(s);
      double frac;
      if (s.isMinTrusted()) {
        frac = overlapFraction(segMin, segMax, lo, hi, loInclusive, hiInclusive);
      } else if (segMax > 0) {
        // Numeric null-sentinel: the stored min may be the type minimum. Use the same estimative
        // symmetric bound as the aggregated path ([-|max|, max]) rather than the polluted value.
        frac = overlapFraction(-Math.abs(segMax), segMax, lo, hi, loInclusive, hiInclusive);
      } else {
        // Untrusted min with a non-positive max gives no reliable lower bound — never reuse the
        // polluted min; use the aggregated fallback fraction for this segment (mirrors
        // PinotRelMdSelectivity#rangeSelectivity returning FALLBACK_RANGE_SELECTIVITY here).
        frac = UNTRUSTED_FALLBACK_FRACTION;
      }
      matchRows.put(s.getSegmentId(), td * frac);
    }
    return matchRows.isEmpty() ? ConjunctEstimate.scalar(ctx.aggregated(node))
        : ConjunctEstimate.pruned(matchRows);
  }

  // --------------------------------------------------------------------------
  // String leaf
  // --------------------------------------------------------------------------

  private ConjunctEstimate stringLeaf(RexCall call, EstimationContext ctx, String colName,
      SqlKind kind, RexLiteral literal) {
    String v = literal.getValueAs(String.class);
    if (v == null) {
      return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    List<SegmentColumnStat> all =
        ctx._provider.getSegmentColumnStats(ctx._tableName, colName, MAX_STRING_FETCH);
    if (all.isEmpty() || all.size() >= MAX_STRING_FETCH) {
      // Empty → no data; full → may have missed segments. Either way, do not prune.
      return ConjunctEstimate.scalar(ctx.aggregated(call));
    }
    boolean equality = kind == SqlKind.EQUALS;
    Long2DoubleOpenHashMap matchRows = new Long2DoubleOpenHashMap();
    for (SegmentColumnStat s : all) {
      String segMin = s.getMinValue();
      String segMax = s.getMaxValue();
      if (segMin == null || segMax == null) {
        return ConjunctEstimate.scalar(ctx.aggregated(call));
      }
      // Compare with Java's String comparator (server semantics), not the store's byte order.
      boolean overlaps;
      if (equality) {
        // Honor minTrusted: an untrusted min must not exclude the segment on its lower bound.
        boolean geMin = !s.isMinTrusted() || segMin.compareTo(v) <= 0;
        overlaps = geMin && segMax.compareTo(v) >= 0;
      } else {
        overlaps = stringRangeOverlaps(segMin, segMax, kind, v, s.isMinTrusted());
      }
      if (!overlaps) {
        continue;
      }
      if (matchRows.size() >= MAX_SURVIVORS) {
        return ConjunctEstimate.scalar(ctx.aggregated(call));
      }
      long td = s.getTotalDocs();
      if (td <= 0) {
        continue;
      }
      double frac;
      if (equality) {
        if (s.getNdv() < 1) {
          return ConjunctEstimate.scalar(ctx.aggregated(call));
        }
        frac = 1.0 / s.getNdv();
      } else {
        // No string distribution metric → use a bounded default within-segment fraction for
        // surviving segments. The win is the segment exclusion, not the residual.
        frac = UNTRUSTED_FALLBACK_FRACTION;
      }
      ctx.recordTotalDocs(s);
      matchRows.put(s.getSegmentId(), td * frac);
    }
    return matchRows.isEmpty() ? ConjunctEstimate.scalar(ctx.aggregated(call))
        : ConjunctEstimate.pruned(matchRows);
  }

  private static boolean stringRangeOverlaps(String segMin, String segMax, SqlKind kind, String v,
      boolean minTrusted) {
    switch (kind) {
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
        // some value >= v exists iff segMax >= v
        return segMax.compareTo(v) >= 0;
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
        // some value <= v exists iff segMin <= v (untrusted min cannot exclude)
        return !minTrusted || segMin.compareTo(v) <= 0;
      default:
        return true;
    }
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  /// Fraction of a segment's `[segMin, segMax]` range satisfying `[lo, hi]`, assuming a uniform
  /// distribution. Returns 1.0 for a degenerate (single-value) segment that satisfies the window.
  static double overlapFraction(double segMin, double segMax, double lo, double hi,
      boolean loInclusive, boolean hiInclusive) {
    if (segMax < segMin) {
      return 0.0;
    }
    double overlapLo = Math.max(segMin, lo);
    double overlapHi = Math.min(segMax, hi);
    if (segMin == segMax) {
      boolean geLo = loInclusive ? segMin >= lo : segMin > lo;
      boolean leHi = hiInclusive ? segMin <= hi : segMin < hi;
      return (geLo && leHi) ? 1.0 : 0.0;
    }
    if (overlapHi < overlapLo) {
      return 0.0;
    }
    double frac = (overlapHi - overlapLo) / (segMax - segMin);
    return Math.min(1.0, Math.max(0.0, frac));
  }

  /// Intersection of the maps' segment-id key sets, seeded from the smallest for efficiency.
  private static LongSet intersectKeys(List<Long2DoubleOpenHashMap> maps) {
    Long2DoubleOpenHashMap smallest = maps.get(0);
    for (Long2DoubleOpenHashMap m : maps) {
      if (m.size() < smallest.size()) {
        smallest = m;
      }
    }
    LongOpenHashSet result = new LongOpenHashSet(smallest.keySet());
    for (Long2DoubleOpenHashMap m : maps) {
      if (m != smallest) {
        result.retainAll(m.keySet());
      }
    }
    return result;
  }

  /// Union of the maps' segment-id key sets.
  private static LongSet unionKeys(List<Long2DoubleOpenHashMap> maps) {
    LongOpenHashSet result = new LongOpenHashSet();
    for (Long2DoubleOpenHashMap m : maps) {
      result.addAll(m.keySet());
    }
    return result;
  }

  private static boolean usable(SegmentColumnStat s) {
    StatConfidence c = s.getNdvConfidence();
    return c == StatConfidence.EXACT || c == StatConfidence.ESTIMATED;
  }

  private static boolean isNumeric(@Nullable FieldSpec.DataType dataType) {
    if (dataType == null) {
      return false;
    }
    switch (dataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }

  private static SqlKind flip(SqlKind kind) {
    switch (kind) {
      case GREATER_THAN:
        return SqlKind.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN_OR_EQUAL;
      case LESS_THAN:
        return SqlKind.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      default:
        return kind;
    }
  }

  @Nullable
  private static Double literalToDouble(RexLiteral literal) {
    switch (literal.getType().getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case REAL:
      case DOUBLE:
      case DECIMAL: {
        Number n = (Number) literal.getValue();
        return n != null ? n.doubleValue() : null;
      }
      default:
        return null;
    }
  }

  @Nullable
  private static Double parseDouble(@Nullable String value) {
    if (value == null) {
      return null;
    }
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static double toDouble(Object endpoint) {
    if (endpoint instanceof BigDecimal) {
      return ((BigDecimal) endpoint).doubleValue();
    }
    if (endpoint instanceof Number) {
      return ((Number) endpoint).doubleValue();
    }
    return Double.NaN;
  }

  private static double clamp(double v) {
    return Math.min(1.0, Math.max(0.0, v));
  }

  // --------------------------------------------------------------------------
  // Internal types
  // --------------------------------------------------------------------------

  /// Per-call mutable context, carrying the global `segmentId → totalDocs` map populated as leaves
  /// fetch. `totalDocs` is segment-invariant, so it is held once here rather than on every node.
  private static final class EstimationContext {
    final String _tableName;
    final PinotStatisticsProvider _provider;
    final Function<RexInputRef, String> _colNameResolver;
    final Function<String, FieldSpec.DataType> _dataTypeResolver;
    final ToDoubleFunction<RexNode> _aggregatedSelectivity;
    final Long2LongOpenHashMap _totalDocs = new Long2LongOpenHashMap();

    EstimationContext(String tableName, PinotStatisticsProvider provider,
        Function<RexInputRef, String> colNameResolver,
        Function<String, FieldSpec.DataType> dataTypeResolver,
        ToDoubleFunction<RexNode> aggregatedSelectivity) {
      _tableName = tableName;
      _provider = provider;
      _colNameResolver = colNameResolver;
      _dataTypeResolver = dataTypeResolver;
      _aggregatedSelectivity = aggregatedSelectivity;
      _totalDocs.defaultReturnValue(-1);
    }

    double aggregated(RexNode node) {
      return _aggregatedSelectivity.applyAsDouble(node);
    }

    void recordTotalDocs(SegmentColumnStat s) {
      if (s.getTotalDocs() > 0) {
        _totalDocs.putIfAbsent(s.getSegmentId(), s.getTotalDocs());
      }
    }
  }

  /// Result of processing a node: either a pruned `segmentId → matched-rows` map, or a non-pruning
  /// scalar selectivity that applies uniformly without restricting the survivor set.
  private static final class ConjunctEstimate {
    final boolean _pruned;
    @Nullable
    final Long2DoubleOpenHashMap _matchRows;
    final double _scalarSelectivity;

    private ConjunctEstimate(boolean pruned, @Nullable Long2DoubleOpenHashMap matchRows,
        double scalarSelectivity) {
      _pruned = pruned;
      _matchRows = matchRows;
      _scalarSelectivity = scalarSelectivity;
    }

    static ConjunctEstimate pruned(Long2DoubleOpenHashMap matchRows) {
      return new ConjunctEstimate(true, matchRows, 1.0);
    }

    static ConjunctEstimate scalar(double selectivity) {
      return new ConjunctEstimate(false, null, selectivity);
    }
  }
}
