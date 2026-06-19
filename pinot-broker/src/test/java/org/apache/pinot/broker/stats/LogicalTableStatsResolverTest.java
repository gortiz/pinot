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
package org.apache.pinot.broker.stats;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Unit tests for [LogicalTableStatsResolver].
///
/// Each test method gets a fresh temporary directory and a new [SqliteStatsStore] and
/// [LogicalTableStatsResolver]. Table configs and time boundaries are injected as simple
/// lambdas.
public class LogicalTableStatsResolverTest {

  private static final String RAW_TABLE = "myTable";
  private static final String OFFLINE_TABLE = "myTable_OFFLINE";
  private static final String REALTIME_TABLE = "myTable_REALTIME";
  private static final double DELTA = 1e-6;

  private Path _tempDir;
  private SqliteStatsStore _store;
  private LogicalTableStatsResolver _resolver;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("logical-stats-resolver-test-");
    _store = new SqliteStatsStore(_tempDir);
    _store.init();
    _resolver = new LogicalTableStatsResolver(_store);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    if (_store != null) {
      _store.close();
    }
    deleteRecursively(_tempDir);
  }

  // ---------------------------------------------------------------------------
  // Offline-only raw lookup
  // ---------------------------------------------------------------------------

  @Test
  public void testRawLookupOfflineOnly()
      throws Exception {
    // Only offline segments exist
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("seg1", 1L, 500L, 4096L, 0L, 100L, false),
        row("seg2", 2L, 300L, 2048L, 100L, 200L, false)
    ));

    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats, "Stats must be present for offline-only raw lookup");
    assertEquals(stats.getRowCount(), 800L, "Row count should be sum of offline segments");
    assertEquals(stats.getTableSizeBytes(), 6144L);
    // Only offline → no ESTIMATED downgrade from hybrid merge
    assertEquals(stats.getRowCountConfidence(), StatConfidence.EXACT);
  }

  // ---------------------------------------------------------------------------
  // Realtime-only raw lookup
  // ---------------------------------------------------------------------------

  @Test
  public void testRawLookupRealtimeOnly()
      throws Exception {
    // Only realtime (committed) segments exist, no consuming
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 10L, 200L, 1024L, 50L, 150L, false),
        row("rtSeg2", 11L, 100L, 512L, 150L, 250L, false)
    ));

    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats, "Stats must be present for realtime-only raw lookup");
    assertEquals(stats.getRowCount(), 300L);
    // No upsert, no consuming → EXACT
    assertEquals(stats.getRowCountConfidence(), StatConfidence.EXACT);
  }

  // ---------------------------------------------------------------------------
  // Suffixed physical lookups
  // ---------------------------------------------------------------------------

  @Test
  public void testSuffixedOfflineLookup()
      throws Exception {
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("seg1", 1L, 1000L, 8192L, 0L, 200L, false)
    ));

    TableStatistics stats = _resolver.getTableStats(OFFLINE_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 1000L);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.EXACT);
  }

  @Test
  public void testSuffixedRealtimeLookupNoAdjustment()
      throws Exception {
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 1L, 400L, 2048L, 0L, 100L, false)
    ));
    // No table config provider → no upsert/dedup detection; no consuming
    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 400L);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.EXACT);
  }

  // ---------------------------------------------------------------------------
  // Hybrid with boundary: no double-count
  // ---------------------------------------------------------------------------

  @Test
  public void testHybridWithBoundary()
      throws Exception {
    // Offline segments: [0,100ms) and [100,200ms)
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("offSeg1", 1L, 100L, 1024L, 0L, 100L, false),
        row("offSeg2", 2L, 200L, 2048L, 100L, 200L, false)
    ));
    // Realtime segments: [150ms, 300ms)
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 10L, 150L, 1024L, 150L, 300L, false)
    ));

    // Boundary at 150ms:
    //   offline contribution: rows whose segments overlap [MIN, 150) = seg1 (full 100) + seg2 (partial 100→150 = 50%)
    //   realtime contribution: rows whose segments overlap [150, MAX) = rtSeg1 (full 150)
    long boundaryMs = 150L;
    _resolver.setTimeBoundaryMsProvider(rawName -> RAW_TABLE.equals(rawName) ? boundaryMs : null);

    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats);

    // Plain sum would be 100+200+150 = 450; with boundary it should be less (no double-count)
    // offline estimate for [MIN, 150): seg1 full=100, seg2 partial (100→150 out of 100→200 = 50%) = 100
    //   → offline = 200
    // realtime estimate for [150, MAX): rtSeg1 full = 150
    //   → merged = 200 + 150 = 350
    assertEquals(stats.getRowCount(), 350L,
        "Hybrid row count should avoid double-counting via boundary split");
    assertEquals(stats.getRowCountConfidence(), StatConfidence.ESTIMATED);

    // Plain sum of sizes is acceptable
    assertEquals(stats.getTableSizeBytes(), 1024L + 2048L + 1024L);
    assertEquals(stats.getSizeConfidence(), StatConfidence.ESTIMATED);
  }

  // ---------------------------------------------------------------------------
  // Hybrid without boundary: plain sum, ESTIMATED
  // ---------------------------------------------------------------------------

  @Test
  public void testHybridWithoutBoundary()
      throws Exception {
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("offSeg1", 1L, 100L, 1024L, 0L, 100L, false)
    ));
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 10L, 200L, 2048L, 50L, 200L, false)
    ));
    // No boundary provider → plain sum
    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCount(), 300L, "Without boundary, plain sum should be used");
    assertEquals(stats.getRowCountConfidence(), StatConfidence.ESTIMATED,
        "Without boundary, confidence must be ESTIMATED");
  }

  // ---------------------------------------------------------------------------
  // Upsert realtime: LOW confidence (raw and suffixed)
  // ---------------------------------------------------------------------------

  @Test
  public void testUpsertRealtimeConfidenceLowSuffixed()
      throws Exception {
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 1L, 500L, 4096L, 0L, 100L, false)
    ));

    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig upsertTable = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE)
        .setUpsertConfig(upsertConfig).build();
    _resolver.setTableConfigProvider(name -> REALTIME_TABLE.equals(name) ? upsertTable : null);

    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.LOW,
        "Upsert realtime table must report LOW confidence");
    assertEquals(stats.getRowCount(), 500L);
  }

  @Test
  public void testUpsertRealtimeConfidenceLowRaw()
      throws Exception {
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 1L, 500L, 4096L, 0L, 100L, false)
    ));
    // No offline table stats

    TableConfig upsertTable = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE)
        .setUpsertConfig(upsertTable(UpsertConfig.Mode.FULL)).build();
    _resolver.setTableConfigProvider(name -> REALTIME_TABLE.equals(name) ? upsertTable : null);

    TableStatistics stats = _resolver.getTableStats(RAW_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.LOW,
        "Upsert realtime raw lookup must report LOW confidence");
  }

  // ---------------------------------------------------------------------------
  // Dedup realtime: LOW confidence
  // ---------------------------------------------------------------------------

  @Test
  public void testDedupRealtimeConfidenceLow()
      throws Exception {
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 1L, 300L, 2048L, 0L, 100L, false)
    ));

    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setDedupEnabled(true);
    TableConfig dedupTable = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE)
        .setDedupConfig(dedupConfig).build();
    _resolver.setTableConfigProvider(name -> REALTIME_TABLE.equals(name) ? dedupTable : null);

    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.LOW,
        "Dedup realtime table must report LOW confidence");
  }

  // ---------------------------------------------------------------------------
  // Consuming present: ESTIMATED confidence
  // ---------------------------------------------------------------------------

  @Test
  public void testConsumingSegmentDowngradesConfidence()
      throws Exception {
    // One committed + one consuming segment
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 1L, 400L, 2048L, 0L, 100L, false),
        row("rtSeg2_consuming", 2L, -1L, -1L, 100L, -1L, true)
    ));

    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    // Row count only includes committed rows (consuming excluded from sum by store)
    assertEquals(stats.getRowCount(), 400L);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.ESTIMATED,
        "Consuming segment present must downgrade confidence to ESTIMATED");
  }

  @Test
  public void testNoConsumingSegmentConfidenceExact()
      throws Exception {
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 1L, 400L, 2048L, 0L, 100L, false)
    ));

    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.EXACT,
        "No consuming segment: confidence should remain EXACT");
  }

  // ---------------------------------------------------------------------------
  // LOW wins over ESTIMATED (upsert + consuming)
  // ---------------------------------------------------------------------------

  @Test
  public void testUpsertPlusConsumingIsLow()
      throws Exception {
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 1L, 400L, 2048L, 0L, 100L, false),
        row("rtSeg2_consuming", 2L, -1L, -1L, 100L, -1L, true)
    ));
    UpsertConfig upsertConfig = new UpsertConfig(UpsertConfig.Mode.FULL);
    TableConfig upsertTable = new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE)
        .setUpsertConfig(upsertConfig).build();
    _resolver.setTableConfigProvider(name -> REALTIME_TABLE.equals(name) ? upsertTable : null);

    TableStatistics stats = _resolver.getTableStats(REALTIME_TABLE);
    assertNotNull(stats);
    assertEquals(stats.getRowCountConfidence(), StatConfidence.LOW,
        "LOW must win over ESTIMATED when upsert + consuming both apply");
  }

  // ---------------------------------------------------------------------------
  // estimateRowsInTimeRange: hybrid split at boundary
  // ---------------------------------------------------------------------------

  @Test
  public void testEstimateRowsInTimeRangeHybridSplit()
      throws Exception {
    // Offline: two segments, each 1000 rows, [0,100) and [100,200)
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("offSeg1", 1L, 1000L, 0L, 0L, 100L, false),
        row("offSeg2", 2L, 1000L, 0L, 100L, 200L, false)
    ));
    // Realtime: one segment, 500 rows, [150,300)
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 10L, 500L, 0L, 150L, 300L, false)
    ));

    long boundaryMs = 150L;
    _resolver.setTimeBoundaryMsProvider(rawName -> RAW_TABLE.equals(rawName) ? boundaryMs : null);

    // Query range [0, 300) on hybrid
    // offline contribution for [0, min(300, 150)) = [0, 150):
    //   offSeg1 full [0,100) → 1000 rows
    //   offSeg2 partial [100,150) out of [100,200) → 50% of 1000 = 500 rows
    //   total offline = 1500
    // realtime contribution for [max(0,150), 300) = [150, 300):
    //   rtSeg1 full [150,300) → 500 rows
    // total = 2000
    OptionalLong estimate = _resolver.estimateRowsInTimeRange(RAW_TABLE, 0L, 300L);
    assertTrue(estimate.isPresent());
    assertEquals(estimate.getAsLong(), 2000L,
        "Hybrid estimateRowsInTimeRange should split at boundary");
  }

  @Test
  public void testEstimateRowsInTimeRangePhysicalTable()
      throws Exception {
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("seg1", 1L, 1000L, 0L, 0L, 100L, false),
        row("seg2", 2L, 1000L, 0L, 100L, 200L, false)
    ));

    // Query [50, 150) on offline: seg1 partial + seg2 partial
    // seg1 [0,100) ∩ [50,150) = [50,100), fraction = 50/100 = 0.5, rows = 500
    // seg2 [100,200) ∩ [50,150) = [100,150), fraction = 50/100 = 0.5, rows = 500
    OptionalLong estimate = _resolver.estimateRowsInTimeRange(OFFLINE_TABLE, 50L, 150L);
    assertTrue(estimate.isPresent());
    assertEquals(estimate.getAsLong(), 1000L);
  }

  // ---------------------------------------------------------------------------
  // Null returns when no data
  // ---------------------------------------------------------------------------

  @Test
  public void testGetTableStatsReturnsNullWhenNoData() {
    assertNull(_resolver.getTableStats(RAW_TABLE));
    assertNull(_resolver.getTableStats(OFFLINE_TABLE));
    assertNull(_resolver.getTableStats(REALTIME_TABLE));
  }

  @Test
  public void testEstimateRowsInTimeRangeEmptyWhenNoData() {
    assertFalse(_resolver.estimateRowsInTimeRange(RAW_TABLE, 0L, Long.MAX_VALUE).isPresent());
    assertFalse(_resolver.estimateRowsInTimeRange(OFFLINE_TABLE, 0L, Long.MAX_VALUE).isPresent());
  }

  // ---------------------------------------------------------------------------
  // weakest() helper
  // ---------------------------------------------------------------------------

  @Test
  public void testWeakestHelper() {
    assertEquals(LogicalTableStatsResolver.weakest(StatConfidence.EXACT, StatConfidence.EXACT),
        StatConfidence.EXACT);
    assertEquals(
        LogicalTableStatsResolver.weakest(StatConfidence.EXACT, StatConfidence.ESTIMATED),
        StatConfidence.ESTIMATED);
    assertEquals(
        LogicalTableStatsResolver.weakest(StatConfidence.ESTIMATED, StatConfidence.LOW),
        StatConfidence.LOW);
    assertEquals(
        LogicalTableStatsResolver.weakest(StatConfidence.LOW, StatConfidence.UNKNOWN),
        StatConfidence.UNKNOWN);
    assertEquals(
        LogicalTableStatsResolver.weakest(StatConfidence.LOW, StatConfidence.ESTIMATED),
        StatConfidence.LOW);
  }

  // ---------------------------------------------------------------------------
  // Column-stats: suffixed physical lookup
  // ---------------------------------------------------------------------------

  @Test
  public void testGetColumnStatsSuffixedOffline()
      throws Exception {
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("seg1", 1L, 100L, 1024L, 0L, 100L, false)
    ));
    _store.upsertSegmentColumnStats(OFFLINE_TABLE, Arrays.asList(
        new SegmentColumnStatsRow("seg1", "age", 50L, "5", "95", true, 4.0, 0.0)
    ));

    ColumnStatistics stats = _resolver.getColumnStats(OFFLINE_TABLE, "age");
    assertNotNull(stats, "Column stats must be present for physical table");
    assertEquals(stats.getNdv(), 50L);
    // SqliteStatsStore always returns ESTIMATED for aggregated NDV
    assertEquals(stats.getNdvConfidence(), StatConfidence.ESTIMATED);
    assertTrue(stats.isMinTrusted());
  }

  @Test
  public void testGetColumnStatsReturnsNullWhenAbsent() {
    assertNull(_resolver.getColumnStats(OFFLINE_TABLE, "age"),
        "Must return null when no column stats stored");
    assertNull(_resolver.getColumnStats(RAW_TABLE, "age"),
        "Must return null for raw name with no data");
  }

  // ---------------------------------------------------------------------------
  // Column-stats: raw name — only one side present
  // ---------------------------------------------------------------------------

  @Test
  public void testGetColumnStatsRawOfflineOnly()
      throws Exception {
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("seg1", 1L, 100L, 1024L, 0L, 100L, false)
    ));
    _store.upsertSegmentColumnStats(OFFLINE_TABLE, Arrays.asList(
        new SegmentColumnStatsRow("seg1", "price", 20L, "1", "100", true, 8.0, 0.1)
    ));

    ColumnStatistics stats = _resolver.getColumnStats(RAW_TABLE, "price");
    assertNotNull(stats, "Must return offline stats when only offline side has data");
    assertEquals(stats.getNdv(), 20L);
  }

  @Test
  public void testGetColumnStatsRawRealtimeOnly()
      throws Exception {
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 10L, 200L, 1024L, 50L, 150L, false)
    ));
    _store.upsertSegmentColumnStats(REALTIME_TABLE, Arrays.asList(
        new SegmentColumnStatsRow("rtSeg1", "score", 30L, "10", "90", true, 4.0, 0.0)
    ));

    ColumnStatistics stats = _resolver.getColumnStats(RAW_TABLE, "score");
    assertNotNull(stats, "Must return realtime stats when only realtime side has data");
    assertEquals(stats.getNdv(), 30L);
  }

  // ---------------------------------------------------------------------------
  // Column-stats: raw name — merge two physical tables
  // ---------------------------------------------------------------------------

  @Test
  public void testGetColumnStatsRawMergeWidensRangeAndMaxNdv()
      throws Exception {
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("seg1", 1L, 100L, 1024L, 0L, 100L, false)
    ));
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 10L, 200L, 1024L, 50L, 150L, false)
    ));
    // Offline: NDV=20, min=5, max=80
    _store.upsertSegmentColumnStats(OFFLINE_TABLE, Arrays.asList(
        new SegmentColumnStatsRow("seg1", "age", 20L, "5", "80", true, 4.0, 0.0)
    ));
    // Realtime: NDV=30, min=1, max=95
    _store.upsertSegmentColumnStats(REALTIME_TABLE, Arrays.asList(
        new SegmentColumnStatsRow("rtSeg1", "age", 30L, "1", "95", true, 4.0, 0.0)
    ));

    ColumnStatistics stats = _resolver.getColumnStats(RAW_TABLE, "age");
    assertNotNull(stats, "Merged stats must be non-null when both sides have data");
    // NDV = max(20, 30) = 30
    assertEquals(stats.getNdv(), 30L, "NDV must be max of the two sides");
    // min = min-of-mins (numeric: 1 < 5); SqliteStatsStore returns Double so check numerically
    assertNotNull(stats.getMinValue());
    double minActual = ((Number) stats.getMinValue()).doubleValue();
    assertEquals(minActual, 1.0, DELTA, "Min must be the numerically smaller of the two mins");
    // max = max-of-maxs = 95.0
    assertNotNull(stats.getMaxValue());
    double maxActual = ((Number) stats.getMaxValue()).doubleValue();
    assertEquals(maxActual, 95.0, DELTA, "Max must be the numerically larger of the two maxs");
    // minTrusted = AND = true && true = true
    assertTrue(stats.isMinTrusted(), "minTrusted must be AND of both sides");
    // confidence must be at most ESTIMATED (merging always downgrades)
    assertNotEquals(stats.getNdvConfidence(), StatConfidence.EXACT,
        "Merged confidence must not be EXACT");
  }

  @Test
  public void testGetColumnStatsRawMergeMinTrustedFalseWhenEitherFalse()
      throws Exception {
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("seg1", 1L, 100L, 1024L, 0L, 100L, false)
    ));
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 10L, 200L, 1024L, 50L, 150L, false)
    ));
    // Offline: minTrusted=true; Realtime: minTrusted=false (null-sentinel pollution)
    _store.upsertSegmentColumnStats(OFFLINE_TABLE, Arrays.asList(
        new SegmentColumnStatsRow("seg1", "val", 10L, "0", "100", true, 4.0, 0.0)
    ));
    _store.upsertSegmentColumnStats(REALTIME_TABLE, Arrays.asList(
        new SegmentColumnStatsRow("rtSeg1", "val", 15L, "-2147483648", "100", false, 4.0, 0.0)
    ));

    ColumnStatistics stats = _resolver.getColumnStats(RAW_TABLE, "val");
    assertNotNull(stats);
    assertFalse(stats.isMinTrusted(),
        "minTrusted must be false when either side has minTrusted=false");
  }

  @Test
  public void testGetColumnStatsRawMergeConfidenceIsWeakest()
      throws Exception {
    insertSegments(OFFLINE_TABLE, Arrays.asList(
        row("seg1", 1L, 100L, 1024L, 0L, 100L, false)
    ));
    insertSegments(REALTIME_TABLE, Arrays.asList(
        row("rtSeg1", 10L, 200L, 1024L, 50L, 150L, false)
    ));
    // Even if both physical stores report EXACT, the merged result must be at most ESTIMATED
    _store.upsertSegmentColumnStats(OFFLINE_TABLE, Arrays.asList(
        new SegmentColumnStatsRow("seg1", "v", 10L, "0", "50", true, 4.0, 0.0)
    ));
    _store.upsertSegmentColumnStats(REALTIME_TABLE, Arrays.asList(
        new SegmentColumnStatsRow("rtSeg1", "v", 10L, "0", "50", true, 4.0, 0.0)
    ));

    ColumnStatistics stats = _resolver.getColumnStats(RAW_TABLE, "v");
    assertNotNull(stats);
    assertEquals(stats.getNdvConfidence(), StatConfidence.ESTIMATED,
        "Merged confidence from two EXACT sides must be ESTIMATED");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private void insertSegments(String tableNameWithType, List<SegmentStatsRow> rows)
      throws StatsStoreException {
    _store.upsertSegmentStats(tableNameWithType, rows);
  }

  private static SegmentStatsRow row(String segName, long crc, long totalDocs, long sizeBytes,
      long startMs, long endMs, boolean consuming) {
    return new SegmentStatsRow(segName, crc, totalDocs, sizeBytes, startMs, endMs, consuming);
  }

  private static UpsertConfig upsertTable(UpsertConfig.Mode mode) {
    return new UpsertConfig(mode);
  }

  private static void deleteRecursively(Path dir)
      throws IOException {
    if (dir == null || !Files.exists(dir)) {
      return;
    }
    try (var stream = Files.walk(dir)) {
      stream.sorted(Comparator.reverseOrder())
          .forEach(p -> {
            try {
              Files.deleteIfExists(p);
            } catch (IOException e) {
              // ignore
            }
          });
    }
  }
}
