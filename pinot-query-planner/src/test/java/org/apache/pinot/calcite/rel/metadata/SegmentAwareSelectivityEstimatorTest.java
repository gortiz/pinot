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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.spi.stats.ColumnPredicate;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.PinotStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.SegmentColumnStat;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests for [SegmentAwareSelectivityEstimator], exercised end-to-end through the Calcite metadata
/// query the same way the planner consults it: build a real [Filter] via SQL compilation, install
/// the estimator on the thread (as `QueryEnvironment.optimize` does), then read selectivity.
///
/// The per-segment prune is simulated by [FakeProvider] (the real SQL prune is covered by the store
/// test); this test focuses on the estimator's composition, restricted-sum, and fallback logic.
public class SegmentAwareSelectivityEstimatorTest {

  private static final String TABLE_NAME = "myTable";
  private static final String TIME_COL = "ts";
  private static final String COL_A = "colA";
  private static final String COL_B = "colB";
  private static final String STR_COL = "name";
  private static final double DELTA = 1e-6;

  @AfterMethod
  public void tearDown() {
    SegmentAwareSelectivityEstimator.clear();
  }

  // --------------------------------------------------------------------------
  // overlapFraction unit tests (no Calcite)
  // --------------------------------------------------------------------------

  @Test
  public void testOverlapFractionFullyInside() {
    // [5,7) over segment [5,10] → 2/5 = 0.4
    assertEquals(SegmentAwareSelectivityEstimator.overlapFraction(5, 10, 5, 7, true, false), 0.4,
        DELTA);
  }

  @Test
  public void testOverlapFractionNoOverlap() {
    // [5,7) over segment [7,15] → 0 (segment starts at the exclusive upper bound)
    assertEquals(SegmentAwareSelectivityEstimator.overlapFraction(7, 15, 5, 7, true, false), 0.0,
        DELTA);
  }

  @Test
  public void testOverlapFractionDegenerateSingleValueMatches() {
    // single-value segment [6,6] satisfying [5,10] → 1.0
    assertEquals(SegmentAwareSelectivityEstimator.overlapFraction(6, 6, 5, 10, true, true), 1.0,
        DELTA);
  }

  @Test
  public void testOverlapFractionDegenerateSingleValueExcluded() {
    // single-value segment [6,6] not satisfying [7,10] → 0.0
    assertEquals(SegmentAwareSelectivityEstimator.overlapFraction(6, 6, 7, 10, true, true), 0.0,
        DELTA);
  }

  // --------------------------------------------------------------------------
  // Numeric equality prune
  // --------------------------------------------------------------------------

  /// `colA = 6` over seg1[0,10] ndv10 docs100, seg2[5,10] ndv5 docs1000, seg3[7,15] ndv7 docs10000.
  /// seg3 is pruned (6 < 7), so estRows = 100/10 + 1000/5 = 210 → selectivity 210/11100.
  @Test
  public void testEqualityPrunesNonMatchingSegment() {
    FakeProvider provider = threeSegmentProvider();
    double sel = selectivityWithEstimator(numericSchema(), provider,
        "SELECT " + COL_A + " FROM " + TABLE_NAME + " WHERE " + COL_A + " = 6");
    assertEquals(sel, 210.0 / 11100.0, DELTA);
  }

  /// `colA > 7` keeps all three segments but weights them by the within-segment overlap fraction:
  /// seg1 0.3·100=30, seg2 0.6·1000=600, seg3 1.0·10000=10000 → 10630/11100.
  @Test
  public void testRangeGreaterThanWeightsSurvivors() {
    FakeProvider provider = threeSegmentProvider();
    double sel = selectivityWithEstimator(numericSchema(), provider,
        "SELECT " + COL_A + " FROM " + TABLE_NAME + " WHERE " + COL_A + " > 7");
    assertEquals(sel, 10630.0 / 11100.0, DELTA);
  }

  // --------------------------------------------------------------------------
  // AND intersection across columns
  // --------------------------------------------------------------------------

  /// `colA = 6 AND colB = 3`: survivors(colA)={seg1,seg2}, survivors(colB)={seg2,seg3} → only seg2
  /// survives the AND. estRows = docs(seg2) · 1/ndvA(seg2) · 1/ndvB(seg2) = 1000 · 1/5 · 1/4 = 50.
  @Test
  public void testAndIntersectionAcrossColumns() {
    FakeProvider provider = threeSegmentProvider();
    // colB: seg2[0,5] ndv4 docs1000, seg3[0,5] ndv8 docs10000 (both admit value 3)
    provider.addColumn(COL_B, List.of(
        seg("seg2", 0, 5, 4, 1000),
        seg("seg3", 0, 5, 8, 10000)));
    double sel = selectivityWithEstimator(twoNumericSchema(), provider,
        "SELECT " + COL_A + " FROM " + TABLE_NAME + " WHERE " + COL_A + " = 6 AND " + COL_B + " = 3");
    // Only seg2 is in both survivor sets.
    double expected = (1000.0 * (1.0 / 5) * (1.0 / 4)) / 11100.0;
    assertEquals(sel, expected, DELTA);
  }

  // --------------------------------------------------------------------------
  // OR inclusion-exclusion
  // --------------------------------------------------------------------------

  /// `colA = 6 OR colB = 3` shares seg2. Inclusion-exclusion avoids double-counting seg2.
  @Test
  public void testOrInclusionExclusion() {
    FakeProvider provider = threeSegmentProvider();
    provider.addColumn(COL_B, List.of(
        seg("seg2", 0, 5, 4, 1000),
        seg("seg3", 0, 5, 8, 10000)));
    double sel = selectivityWithEstimator(twoNumericSchema(), provider,
        "SELECT " + COL_A + " FROM " + TABLE_NAME + " WHERE " + COL_A + " = 6 OR " + COL_B + " = 3");
    // survivors(colA=6)={seg1:1/10, seg2:1/5}; survivors(colB=3)={seg2:1/4, seg3:1/8}.
    // seg1: 1/10. seg2: 1/5 + 1/4 - (1/5)(1/4) = 0.2+0.25-0.05 = 0.4. seg3: 1/8.
    double estRows = 100 * (1.0 / 10) + 1000 * 0.4 + 10000 * (1.0 / 8);
    assertEquals(sel, estRows / 11100.0, DELTA);
  }

  // --------------------------------------------------------------------------
  // Fallbacks
  // --------------------------------------------------------------------------

  /// No per-segment data → estimator returns empty → aggregated 1/ndv path is used.
  @Test
  public void testNoSegmentDataFallsBackToAggregated() {
    FakeProvider provider = new FakeProvider(11100);
    // Aggregated NDV only (no per-segment rows registered).
    provider.setAggregatedNdv(COL_A, 10);
    double sel = selectivityWithEstimator(numericSchema(), provider,
        "SELECT " + COL_A + " FROM " + TABLE_NAME + " WHERE " + COL_A + " = 6");
    assertEquals(sel, 1.0 / 10, DELTA, "Empty per-segment data must fall back to aggregated 1/NDV");
  }

  /// More than MAX_SURVIVORS surviving segments → cost guard trips → aggregated fallback.
  @Test
  public void testCostGuardFallsBackToAggregated() {
    FakeProvider provider = new FakeProvider(1_000_000);
    List<SegRec> many = new ArrayList<>();
    for (int i = 0; i <= SegmentAwareSelectivityEstimator.MAX_SURVIVORS; i++) {
      many.add(seg("seg" + i, 0, 10, 10, 100));
    }
    provider.addColumn(COL_A, many);
    provider.setAggregatedNdv(COL_A, 10);
    double sel = selectivityWithEstimator(numericSchema(), provider,
        "SELECT " + COL_A + " FROM " + TABLE_NAME + " WHERE " + COL_A + " = 6");
    assertEquals(sel, 1.0 / 10, DELTA, "Exceeding the cost guard must fall back to aggregated 1/NDV");
  }

  /// With no estimator installed (feature off) selectivity equals the aggregated path.
  @Test
  public void testFeatureOffUsesAggregatedPath() {
    FakeProvider provider = threeSegmentProvider();
    provider.setAggregatedNdv(COL_A, 10);
    // Do NOT install the estimator.
    Filter filter = compileFilter(numericSchema(), provider,
        "SELECT " + COL_A + " FROM " + TABLE_NAME + " WHERE " + COL_A + " = 6");
    RelMetadataQuery mq = filter.getCluster().getMetadataQuery();
    Double sel = mq.getSelectivity(filter, null);
    assertNotNull(sel);
    assertEquals(sel, 1.0 / 10, DELTA, "Feature off must use aggregated 1/NDV, not segment-aware");
  }

  // --------------------------------------------------------------------------
  // String equality prune
  // --------------------------------------------------------------------------

  /// String equality prunes segments whose [min,max] cannot contain the literal.
  /// `name = 'm'`: seg1['a','f'] excluded ('m' > 'f'), seg2['g','z'] kept → estRows = 1000/5.
  @Test
  public void testStringEqualityPrune() {
    FakeProvider provider = new FakeProvider(11000);
    provider.addStringColumn(STR_COL, List.of(
        strSeg("seg1", "a", "f", 10, 1000),
        strSeg("seg2", "g", "z", 5, 1000),
        strSeg("seg3", "0", "9", 7, 9000)));
    provider.setAggregatedNdv(STR_COL, 10);
    double sel = selectivityWithEstimator(stringSchema(), provider,
        "SELECT " + STR_COL + " FROM " + TABLE_NAME + " WHERE " + STR_COL + " = 'm'");
    // Only seg2 admits 'm'. estRows = 1000 / 5 = 200.
    assertEquals(sel, 200.0 / 11000.0, DELTA);
  }

  /// Untrusted min (numeric null-sentinel) must NOT be used as the lower bound: with segMax>0 the
  /// estimator uses the symmetric `[-max, max]` bound. seg[sentinel,100] minTrusted=false, `colA>0`
  /// → overlap of [0,+inf) with [-100,100] = (100-0)/(100-(-100)) = 0.5 → estRows 500/1000.
  @Test
  public void testUntrustedMinUsesSymmetricBound() {
    FakeProvider provider = new FakeProvider(1000);
    provider.addColumn(COL_A, List.of(
        segUntrusted("seg1", Integer.MIN_VALUE, 100, 10, 1000)));
    provider.setAggregatedNdv(COL_A, 10);
    double sel = selectivityWithEstimator(numericSchema(), provider,
        "SELECT " + COL_A + " FROM " + TABLE_NAME + " WHERE " + COL_A + " > 0");
    assertEquals(sel, 0.5, DELTA);
  }

  /// String range prunes segments that cannot overlap and assigns surviving segments a bounded
  /// default within-segment fraction. `name > 'm'`: only seg2['g','z'] survives → 0.25·1000.
  @Test
  public void testStringRangePruneWithDefaultFraction() {
    FakeProvider provider = new FakeProvider(11000);
    provider.addStringColumn(STR_COL, List.of(
        strSeg("seg1", "a", "f", 10, 1000),
        strSeg("seg2", "g", "z", 5, 1000),
        strSeg("seg3", "0", "9", 7, 9000)));
    provider.setAggregatedNdv(STR_COL, 10);
    double sel = selectivityWithEstimator(stringSchema(), provider,
        "SELECT " + STR_COL + " FROM " + TABLE_NAME + " WHERE " + STR_COL + " > 'm'");
    assertEquals(sel, (0.25 * 1000) / 11000.0, DELTA);
  }

  /// Segment-aware estimate must be a probability in [0,1].
  @Test
  public void testEstimateIsBounded() {
    FakeProvider provider = threeSegmentProvider();
    double sel = selectivityWithEstimator(numericSchema(), provider,
        "SELECT " + COL_A + " FROM " + TABLE_NAME + " WHERE " + COL_A + " > 7");
    assertTrue(sel >= 0.0 && sel <= 1.0, "Selectivity must be within [0,1]");
  }

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  private double selectivityWithEstimator(Schema schema, FakeProvider provider, String sql) {
    Filter filter = compileFilter(schema, provider, sql);
    SegmentAwareSelectivityEstimator.install();
    try {
      RelMetadataQuery mq = filter.getCluster().getMetadataQuery();
      Double sel = mq.getSelectivity(filter, null);
      assertNotNull(sel, "Selectivity must not be null");
      return sel;
    } finally {
      SegmentAwareSelectivityEstimator.clear();
    }
  }

  private Filter compileFilter(Schema schema, PinotStatisticsProvider provider, String sql) {
    QueryEnvironment env = buildEnv(schema, provider);
    QueryEnvironment.CompiledQuery compiled = env.compile(sql);
    Filter filter = findFirstFilter(compiled.getRelNode());
    assertNotNull(filter, "A Filter node must be present in the plan");
    return filter;
  }

  private static QueryEnvironment buildEnv(Schema schema, PinotStatisticsProvider statsProvider) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    factory.registerTable(schema, TABLE_NAME);
    factory.registerSegment(1, TABLE_NAME + "_OFFLINE", "seg1");
    TableCache tableCache = factory.buildTableCache();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL).build();
    when(tableCache.getTableConfig(TABLE_NAME + "_OFFLINE")).thenReturn(tableConfig);
    return new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(tableCache)
        .statisticsProvider(statsProvider)
        .build());
  }

  private static Schema numericSchema() {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension(COL_A, FieldSpec.DataType.INT, 0)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .setSchemaName(TABLE_NAME)
        .build();
  }

  private static Schema twoNumericSchema() {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension(COL_A, FieldSpec.DataType.INT, 0)
        .addSingleValueDimension(COL_B, FieldSpec.DataType.INT, 0)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .setSchemaName(TABLE_NAME)
        .build();
  }

  private static Schema stringSchema() {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension(STR_COL, FieldSpec.DataType.STRING, "")
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .setSchemaName(TABLE_NAME)
        .build();
  }

  private static FakeProvider threeSegmentProvider() {
    FakeProvider provider = new FakeProvider(11100);
    provider.addColumn(COL_A, List.of(
        seg("seg1", 0, 10, 10, 100),
        seg("seg2", 5, 10, 5, 1000),
        seg("seg3", 7, 15, 7, 10000)));
    provider.setAggregatedNdv(COL_A, 10);
    return provider;
  }

  @Nullable
  private static Filter findFirstFilter(RelNode node) {
    if (node instanceof Filter) {
      return (Filter) node;
    }
    for (RelNode input : node.getInputs()) {
      Filter found = findFirstFilter(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private static SegRec seg(String name, double min, double max, long ndv, long docs) {
    return new SegRec(name, Double.toString(min), Double.toString(max), ndv, docs, true);
  }

  private static SegRec segUntrusted(String name, double min, double max, long ndv, long docs) {
    return new SegRec(name, Double.toString(min), Double.toString(max), ndv, docs, false);
  }

  private static SegRec strSeg(String name, String min, String max, long ndv, long docs) {
    return new SegRec(name, min, max, ndv, docs, true);
  }

  /// A per-segment fixture row.
  private static final class SegRec {
    final String _name;
    final String _min;
    final String _max;
    final long _ndv;
    final long _docs;
    final boolean _minTrusted;

    SegRec(String name, String min, String max, long ndv, long docs, boolean minTrusted) {
      _name = name;
      _min = min;
      _max = max;
      _ndv = ndv;
      _docs = docs;
      _minTrusted = minTrusted;
    }

    SegmentColumnStat toStat() {
      return SegmentColumnStat.fromName(_name, _docs, _ndv, StatConfidence.ESTIMATED, _min, _max,
          _minTrusted);
    }
  }

  /// Hand-written provider simulating the store: the numeric prune filters the fixture by overlap,
  /// the string path returns all rows for Java pruning, and aggregated stats back the fallback path.
  private static final class FakeProvider implements PinotStatisticsProvider {
    private final long _rowCount;
    private final Map<String, List<SegRec>> _numericColumns = new HashMap<>();
    private final Map<String, List<SegRec>> _stringColumns = new HashMap<>();
    private final Map<String, Long> _aggregatedNdv = new HashMap<>();

    FakeProvider(long rowCount) {
      _rowCount = rowCount;
    }

    void addColumn(String column, List<SegRec> segments) {
      _numericColumns.put(column, segments);
    }

    void addStringColumn(String column, List<SegRec> segments) {
      _stringColumns.put(column, segments);
    }

    void setAggregatedNdv(String column, long ndv) {
      _aggregatedNdv.put(column, ndv);
    }

    @Nullable
    @Override
    public TableStatistics getTableStatistics(String tableName) {
      return TableStatistics.builder().rowCount(_rowCount, StatConfidence.EXACT).build();
    }

    @Nullable
    @Override
    public ColumnStatistics getColumnStatistics(String tableName, String columnName) {
      Long ndv = _aggregatedNdv.get(columnName);
      if (ndv == null) {
        return null;
      }
      return ColumnStatistics.builder()
          .columnName(columnName)
          .ndv(ndv, StatConfidence.ESTIMATED)
          .build();
    }

    @Override
    public List<SegmentColumnStat> getSurvivingSegmentColumnStats(String tableName,
        String columnName, ColumnPredicate predicate, int limit) {
      List<SegRec> segs = _numericColumns.get(columnName);
      if (segs == null) {
        return List.of();
      }
      List<SegmentColumnStat> result = new ArrayList<>();
      for (SegRec s : segs) {
        double min = Double.parseDouble(s._min);
        double max = Double.parseDouble(s._max);
        // Overlap: keep where min <= hi AND max >= lo.
        if (min <= predicate.getHi() && max >= predicate.getLo()) {
          result.add(s.toStat());
          if (result.size() >= limit) {
            break;
          }
        }
      }
      return result;
    }

    @Override
    public List<SegmentColumnStat> getSegmentColumnStats(String tableName, String columnName,
        int limit) {
      List<SegRec> segs = _stringColumns.get(columnName);
      if (segs == null) {
        return List.of();
      }
      List<SegmentColumnStat> result = new ArrayList<>();
      for (SegRec s : segs) {
        result.add(s.toStat());
        if (result.size() >= limit) {
          break;
        }
      }
      return result;
    }
  }
}
