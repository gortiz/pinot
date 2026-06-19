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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Unit tests for [BrokerPullColumnStatsSource] parsing and field-derivation logic.
///
/// Tests drive the package-visible static helpers directly — no live server or network needed.
public class BrokerPullColumnStatsSourceTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String SEG = "myTable_OFFLINE_0";

  // ---------------------------------------------------------------------------
  // parseSegmentStats — basic happy path
  // ---------------------------------------------------------------------------

  @Test
  public void testParseSegmentStatsIntColumnFixedWidth() {
    ObjectNode segNode = MAPPER.createObjectNode();
    segNode.put("totalDocs", 1000L);
    ArrayNode columns = MAPPER.createArrayNode();
    columns.add(columnNode("age", "INT", 100, 5, 100, 1000, 1000, true, "1", "120"));
    segNode.set("columns", columns);

    List<SegmentColumnStatsRow> rows = BrokerPullColumnStatsSource.parseSegmentStats(SEG, segNode, null);
    assertEquals(rows.size(), 1);
    SegmentColumnStatsRow row = rows.get(0);
    assertEquals(row.getColumnName(), "age");
    assertEquals(row.getNdv(), 100);
    // INT fixed width = 4
    assertEquals(row.getAvgBytesPerValue(), 4.0, 0.001);
    assertTrue(row.isMinTrusted()); // real min value, no schema → defaults true
    assertEquals(row.getNullFraction(), -1.0);
  }

  @Test
  public void testParseSegmentStatsStringColumnVariableWidth() {
    ObjectNode segNode = MAPPER.createObjectNode();
    segNode.put("totalDocs", 500L);
    ArrayNode columns = MAPPER.createArrayNode();
    columns.add(columnNode("name", "STRING", 200, 3, 20, 500, 500, true, "Alice", "Zach"));
    segNode.set("columns", columns);

    List<SegmentColumnStatsRow> rows = BrokerPullColumnStatsSource.parseSegmentStats(SEG, segNode, null);
    assertEquals(rows.size(), 1);
    SegmentColumnStatsRow row = rows.get(0);
    // avgBytes = (3 + 20) / 2 = 11.5
    assertEquals(row.getAvgBytesPerValue(), 11.5, 0.001);
    assertEquals(row.getMinValue(), "Alice");
    assertEquals(row.getMaxValue(), "Zach");
  }

  @Test
  public void testParseSegmentStatsMvColumnAvgEntries() {
    // MV column: 500 total entries over 100 docs → 5 entries/row average
    ObjectNode segNode = MAPPER.createObjectNode();
    segNode.put("totalDocs", 100L);
    ArrayNode columns = MAPPER.createArrayNode();
    // INT MV: totalNumberOfEntries=500, totalDocs=100 → avgEntriesPerRow=5, bytesPerEntry=4
    ObjectNode col = columnNode("tags", "INT", 50, -1, -1, 100, 500, false, null, null);
    col.put("singleValue", false);
    col.put("totalNumberOfEntries", 500L);
    columns.add(col);
    segNode.set("columns", columns);

    List<SegmentColumnStatsRow> rows = BrokerPullColumnStatsSource.parseSegmentStats(SEG, segNode, null);
    assertEquals(rows.size(), 1);
    // INT = 4 bytes * 5 entries/row = 20
    assertEquals(rows.get(0).getAvgBytesPerValue(), 20.0, 0.001);
  }

  // ---------------------------------------------------------------------------
  // parseSegmentStats — missing / absent fields → sentinels
  // ---------------------------------------------------------------------------

  @Test
  public void testParseSegmentStatsMissingCardinalityNdvIsNeg1() {
    ObjectNode segNode = MAPPER.createObjectNode();
    segNode.put("totalDocs", 100L);
    ArrayNode columns = MAPPER.createArrayNode();
    ObjectNode col = MAPPER.createObjectNode();
    col.put("columnName", "x");
    // no cardinality field
    columns.add(col);
    segNode.set("columns", columns);

    List<SegmentColumnStatsRow> rows = BrokerPullColumnStatsSource.parseSegmentStats(SEG, segNode, null);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).getNdv(), -1L);
  }

  @Test
  public void testParseSegmentStatsMissingColumnsEmpty() {
    ObjectNode segNode = MAPPER.createObjectNode();
    segNode.put("totalDocs", 100L);
    // no columns array
    List<SegmentColumnStatsRow> rows = BrokerPullColumnStatsSource.parseSegmentStats(SEG, segNode, null);
    assertTrue(rows.isEmpty());
  }

  @Test
  public void testParseSegmentStatsNullColumnNameSkipped() {
    ObjectNode segNode = MAPPER.createObjectNode();
    segNode.put("totalDocs", 100L);
    ArrayNode columns = MAPPER.createArrayNode();
    ObjectNode col = MAPPER.createObjectNode();
    // no columnName field
    col.put("cardinality", 10);
    columns.add(col);
    segNode.set("columns", columns);

    List<SegmentColumnStatsRow> rows = BrokerPullColumnStatsSource.parseSegmentStats(SEG, segNode, null);
    assertTrue(rows.isEmpty(), "Columns without columnName must be skipped");
  }

  // ---------------------------------------------------------------------------
  // fixedWidthBytes
  // ---------------------------------------------------------------------------

  @Test
  public void testFixedWidthBytes() {
    assertEquals(BrokerPullColumnStatsSource.fixedWidthBytes(DataType.INT), 4.0);
    assertEquals(BrokerPullColumnStatsSource.fixedWidthBytes(DataType.LONG), 8.0);
    assertEquals(BrokerPullColumnStatsSource.fixedWidthBytes(DataType.FLOAT), 4.0);
    assertEquals(BrokerPullColumnStatsSource.fixedWidthBytes(DataType.DOUBLE), 8.0);
    assertEquals(BrokerPullColumnStatsSource.fixedWidthBytes(DataType.BOOLEAN), 1.0);
    assertEquals(BrokerPullColumnStatsSource.fixedWidthBytes(DataType.TIMESTAMP), 8.0);
    assertEquals(BrokerPullColumnStatsSource.fixedWidthBytes(DataType.STRING), -1.0);
    assertEquals(BrokerPullColumnStatsSource.fixedWidthBytes(DataType.BYTES), -1.0);
    assertEquals(BrokerPullColumnStatsSource.fixedWidthBytes(null), -1.0);
  }

  // ---------------------------------------------------------------------------
  // isNullSentinel
  // ---------------------------------------------------------------------------

  @Test
  public void testIsNullSentinelInt() {
    assertTrue(BrokerPullColumnStatsSource.isNullSentinel(DataType.INT,
        String.valueOf(Integer.MIN_VALUE)));
    assertFalse(BrokerPullColumnStatsSource.isNullSentinel(DataType.INT, "0"));
    assertFalse(BrokerPullColumnStatsSource.isNullSentinel(DataType.INT, "abc"));
  }

  @Test
  public void testIsNullSentinelLong() {
    assertTrue(BrokerPullColumnStatsSource.isNullSentinel(DataType.LONG,
        String.valueOf(Long.MIN_VALUE)));
    assertFalse(BrokerPullColumnStatsSource.isNullSentinel(DataType.LONG, "0"));
  }

  @Test
  public void testIsNullSentinelFloat() {
    // Pinot float sentinel = -Float.MAX_VALUE
    assertTrue(BrokerPullColumnStatsSource.isNullSentinel(DataType.FLOAT,
        String.valueOf(-Float.MAX_VALUE)));
    assertFalse(BrokerPullColumnStatsSource.isNullSentinel(DataType.FLOAT, "1.5"));
  }

  @Test
  public void testIsNullSentinelDouble() {
    assertTrue(BrokerPullColumnStatsSource.isNullSentinel(DataType.DOUBLE,
        String.valueOf(-Double.MAX_VALUE)));
    assertFalse(BrokerPullColumnStatsSource.isNullSentinel(DataType.DOUBLE, "3.14"));
  }

  @Test
  public void testIsNullSentinelStringAlwaysFalse() {
    // STRING has no numeric sentinel
    assertFalse(BrokerPullColumnStatsSource.isNullSentinel(DataType.STRING, "anything"));
  }

  // ---------------------------------------------------------------------------
  // computeMinTrusted — with schema
  // ---------------------------------------------------------------------------

  @Test
  public void testComputeMinTrustedNonNullableColumnAlwaysTrue() {
    // FieldSpec._notNull defaults to false (nullable=true), so explicitly mark as NOT NULL
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("age", DataType.INT, true);
    fieldSpec.setNotNull(true);
    Schema schema = new Schema.SchemaBuilder().addField(fieldSpec).build();
    // Even if minValue is Integer.MIN_VALUE, non-nullable column → trusted
    assertTrue(BrokerPullColumnStatsSource.computeMinTrusted("age",
        String.valueOf(Integer.MIN_VALUE), DataType.INT, schema, false));
  }

  @Test
  public void testComputeMinTrustedNullableColumnSentinelMinNotTrusted() {
    Schema schema = buildNullableIntSchema("score");
    // nullable column AND min = Integer.MIN_VALUE → not trusted
    assertFalse(BrokerPullColumnStatsSource.computeMinTrusted("score",
        String.valueOf(Integer.MIN_VALUE), DataType.INT, schema, false));
  }

  @Test
  public void testComputeMinTrustedNullableColumnNormalMinTrusted() {
    Schema schema = buildNullableIntSchema("score");
    // nullable column but min is a real value → trusted
    assertTrue(BrokerPullColumnStatsSource.computeMinTrusted("score", "42", DataType.INT, schema, false));
  }

  @Test
  public void testComputeMinTrustedNoSchemaDefaultsTrue() {
    assertTrue(BrokerPullColumnStatsSource.computeMinTrusted("col", String.valueOf(Integer.MIN_VALUE),
        DataType.INT, null, false));
  }

  @Test
  public void testComputeMinTrustedNullMinValueNotTrusted() {
    Schema schema = buildNullableIntSchema("col");
    // No usable min value → cannot be trusted as a pruning bound.
    assertFalse(BrokerPullColumnStatsSource.computeMinTrusted("col", null, DataType.INT, schema, false));
  }

  @Test
  public void testComputeMinTrustedMinMaxValueInvalidNotTrusted() {
    // Server flagged min/max invalid → not trusted even with a real, non-sentinel min and no schema.
    assertFalse(BrokerPullColumnStatsSource.computeMinTrusted("col", "42", DataType.INT, null, true));
  }

  @Test
  public void testParseSegmentStatsMinMaxValueInvalidUntrusted() {
    ObjectNode segNode = MAPPER.createObjectNode();
    segNode.put("totalDocs", 100L);
    ArrayNode columns = MAPPER.createArrayNode();
    // Real, non-sentinel min/max but server flagged them invalid → minTrusted must be false.
    ObjectNode col = columnNode("age", "INT", 10, 4, 4, 100, 100, true, "5", "95");
    col.put("minMaxValueInvalid", true);
    columns.add(col);
    segNode.set("columns", columns);

    List<SegmentColumnStatsRow> rows = BrokerPullColumnStatsSource.parseSegmentStats(SEG, segNode, null);
    assertEquals(rows.size(), 1);
    assertFalse(rows.get(0).isMinTrusted(), "minMaxValueInvalid=true must force minTrusted=false");
  }

  // ---------------------------------------------------------------------------
  // mergeServerResults — HTTP fan-out merge logic
  // ---------------------------------------------------------------------------

  @Test
  public void testMergeServerResultsDedupFirstWins() {
    // Two replicas return the same segment with different ndv; first one in the list wins.
    String bodyA = oneColumnSegmentBody("seg1", "age", 100);
    String bodyB = oneColumnSegmentBody("seg1", "age", 999);
    List<BrokerPullColumnStatsSource.ServerResult> results = List.of(
        new BrokerPullColumnStatsSource.ServerResult("Server_A", 200, bodyA, null),
        new BrokerPullColumnStatsSource.ServerResult("Server_B", 200, bodyB, null));

    Map<String, List<SegmentColumnStatsRow>> merged =
        BrokerPullColumnStatsSource.mergeServerResults(results, null, "myTable_OFFLINE");
    assertEquals(merged.size(), 1);
    assertEquals(merged.get("seg1").get(0).getNdv(), 100, "First non-error response wins");
  }

  @Test
  public void testMergeServerResults500And200Merge() {
    // One server errors with HTTP 500, the other returns 200 → only the 200's segment is kept.
    String body = oneColumnSegmentBody("seg2", "age", 42);
    List<BrokerPullColumnStatsSource.ServerResult> results = List.of(
        new BrokerPullColumnStatsSource.ServerResult("Server_A", 500, null, null),
        new BrokerPullColumnStatsSource.ServerResult("Server_B", 200, body, null));

    Map<String, List<SegmentColumnStatsRow>> merged =
        BrokerPullColumnStatsSource.mergeServerResults(results, null, "myTable_OFFLINE");
    assertEquals(merged.size(), 1);
    assertEquals(merged.get("seg2").get(0).getNdv(), 42);
  }

  @Test
  public void testMergeServerResultsExceptionAndOthersStillMerge() {
    // One server threw an exception; others still merge.
    String body = oneColumnSegmentBody("seg3", "age", 7);
    List<BrokerPullColumnStatsSource.ServerResult> results = List.of(
        new BrokerPullColumnStatsSource.ServerResult("Server_A", -1, null, new RuntimeException("boom")),
        new BrokerPullColumnStatsSource.ServerResult("Server_B", 200, body, null));

    Map<String, List<SegmentColumnStatsRow>> merged =
        BrokerPullColumnStatsSource.mergeServerResults(results, null, "myTable_OFFLINE");
    assertEquals(merged.size(), 1);
    assertTrue(merged.containsKey("seg3"));
  }

  @Test
  public void testMergeServerResultsEmptySegments() {
    // A 200 response with an empty segment map yields no entries.
    List<BrokerPullColumnStatsSource.ServerResult> results = List.of(
        new BrokerPullColumnStatsSource.ServerResult("Server_A", 200, "{}", null));
    Map<String, List<SegmentColumnStatsRow>> merged =
        BrokerPullColumnStatsSource.mergeServerResults(results, null, "myTable_OFFLINE");
    assertTrue(merged.isEmpty());
  }

  @Test
  public void testMergeServerResultsNoServersEmpty() {
    // When server resolution skips every server (e.g. no admin endpoint), the result list is empty.
    Map<String, List<SegmentColumnStatsRow>> merged =
        BrokerPullColumnStatsSource.mergeServerResults(List.of(), null, "myTable_OFFLINE");
    assertTrue(merged.isEmpty());
  }

  @Test
  public void testMergeServerResultsMalformedBodyOthersStillMerge() {
    // A 200 response with unparseable JSON is skipped; the valid server still merges.
    String validBody = oneColumnSegmentBody("seg4", "age", 11);
    List<BrokerPullColumnStatsSource.ServerResult> results = List.of(
        new BrokerPullColumnStatsSource.ServerResult("Server_A", 200, "not-json{", null),
        new BrokerPullColumnStatsSource.ServerResult("Server_B", 200, validBody, null));

    Map<String, List<SegmentColumnStatsRow>> merged =
        BrokerPullColumnStatsSource.mergeServerResults(results, null, "myTable_OFFLINE");
    assertEquals(merged.size(), 1);
    assertEquals(merged.get("seg4").get(0).getNdv(), 11);
  }

  @Test
  public void testMergeServerResultsMultiSegmentCrossServerDedup() {
    // Server A hosts seg1 + seg2; server B hosts seg2 (duplicate, different ndv) + seg3.
    // First win on the shared seg2; new segments from each server are all kept.
    String bodyA = twoColumnSegmentsBody("seg1", 1, "seg2", 2);
    String bodyB = twoColumnSegmentsBody("seg2", 999, "seg3", 3);
    List<BrokerPullColumnStatsSource.ServerResult> results = List.of(
        new BrokerPullColumnStatsSource.ServerResult("Server_A", 200, bodyA, null),
        new BrokerPullColumnStatsSource.ServerResult("Server_B", 200, bodyB, null));

    Map<String, List<SegmentColumnStatsRow>> merged =
        BrokerPullColumnStatsSource.mergeServerResults(results, null, "myTable_OFFLINE");
    assertEquals(merged.size(), 3);
    assertEquals(merged.get("seg2").get(0).getNdv(), 2, "First server's seg2 wins");
    assertTrue(merged.containsKey("seg1"));
    assertTrue(merged.containsKey("seg3"));
  }

  // ---------------------------------------------------------------------------
  // computeAvgBytes — variable-width fallback
  // ---------------------------------------------------------------------------

  @Test
  public void testComputeAvgBytesVariableWidthNoLengthsUseDefault() {
    ObjectNode col = MAPPER.createObjectNode();
    // no lengthOfShortestElement or lengthOfLongestElement
    double avg = BrokerPullColumnStatsSource.computeAvgBytes(DataType.STRING, true, 100, 100, col);
    assertEquals(avg, FieldSpec.DEFAULT_MAX_LENGTH, 0.001);
  }

  @Test
  public void testComputeAvgBytesVariableWidthOnlyLongest() {
    ObjectNode col = MAPPER.createObjectNode();
    col.put("lengthOfLongestElement", 30);
    double avg = BrokerPullColumnStatsSource.computeAvgBytes(DataType.STRING, true, 100, 100, col);
    assertEquals(avg, 30.0, 0.001);
  }

  @Test
  public void testComputeAvgBytesVariableWidthBothLengths() {
    ObjectNode col = MAPPER.createObjectNode();
    col.put("lengthOfShortestElement", 4);
    col.put("lengthOfLongestElement", 16);
    double avg = BrokerPullColumnStatsSource.computeAvgBytes(DataType.STRING, true, 100, 100, col);
    assertEquals(avg, 10.0, 0.001);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static ObjectNode columnNode(String name, String dataType, int cardinality,
      int shortest, int longest, long totalDocs, long totalEntries, boolean singleValue,
      @Nullable String minValue, @Nullable String maxValue) {
    ObjectNode n = MAPPER.createObjectNode();
    n.put("columnName", name);
    n.put("dataType", dataType);
    n.put("cardinality", cardinality);
    n.put("totalDocs", totalDocs);
    n.put("totalNumberOfEntries", totalEntries);
    n.put("singleValue", singleValue);
    if (shortest >= 0) {
      n.put("lengthOfShortestElement", shortest);
    }
    if (longest >= 0) {
      n.put("lengthOfLongestElement", longest);
    }
    if (minValue != null) {
      n.put("minValue", minValue);
    }
    if (maxValue != null) {
      n.put("maxValue", maxValue);
    }
    return n;
  }

  /// Builds a server-response body for a single segment with one INT column of the given ndv.
  /// Shape: `{"<segment>": {"totalDocs": 100, "columns": [{...}]}}`.
  private static String oneColumnSegmentBody(String segment, String column, int ndv) {
    ObjectNode segNode = MAPPER.createObjectNode();
    segNode.put("totalDocs", 100L);
    ArrayNode columns = MAPPER.createArrayNode();
    columns.add(columnNode(column, "INT", ndv, 4, 4, 100, 100, true, null, null));
    segNode.set("columns", columns);
    ObjectNode root = MAPPER.createObjectNode();
    root.set(segment, segNode);
    return root.toString();
  }

  /// Builds a server-response body containing two segments, each with one INT column of the
  /// given ndv. Shape: `{"<seg1>": {...}, "<seg2>": {...}}`.
  private static String twoColumnSegmentsBody(String seg1, int ndv1, String seg2, int ndv2) {
    ObjectNode root = MAPPER.createObjectNode();
    for (String[] pair : new String[][]{{seg1, String.valueOf(ndv1)}, {seg2, String.valueOf(ndv2)}}) {
      ObjectNode segNode = MAPPER.createObjectNode();
      segNode.put("totalDocs", 100L);
      ArrayNode columns = MAPPER.createArrayNode();
      columns.add(columnNode("age", "INT", Integer.parseInt(pair[1]), 4, 4, 100, 100, true, null, null));
      segNode.set("columns", columns);
      root.set(pair[0], segNode);
    }
    return root.toString();
  }

  /// Builds a schema with a single nullable INT dimension named `columnName`.
  private static Schema buildNullableIntSchema(String columnName) {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec(columnName, DataType.INT, true);
    fieldSpec.setNullable(true);
    return new Schema.SchemaBuilder().addField(fieldSpec).build();
  }
}
