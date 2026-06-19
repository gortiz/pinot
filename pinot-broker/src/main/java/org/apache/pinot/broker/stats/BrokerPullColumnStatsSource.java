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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Closeable;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.http.MultiHttpRequest;
import org.apache.pinot.common.http.MultiHttpRequestResponse;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// [ColumnStatsSource] implementation that fetches per-segment column statistics from servers
/// by fanning out bulk metadata requests to each server that hosts the requested segments.
///
/// ### Server endpoint
/// Reuses the existing server bulk endpoint with no server-side changes:
/// `GET {serverAdminEndpoint}/tables/{tableNameWithType}/segments/metadata?segments=...&columns=*`
/// Each server responds with a `Map<segmentName, segmentMetadataJson>` — only for the segments
/// it actually hosts from the requested set.
///
/// ### Server resolution
/// Uses an injected [ServerSegmentProvider] to map segments → server + admin endpoint. This keeps
/// the class testable without a live cluster. In production the implementation is
/// [HelixServerSegmentProvider], which reads the Helix ExternalView via ZK property store and
/// looks up admin endpoints from the broker's `ServerInstance` map.
///
/// ### Replica deduplication
/// The same segment may be returned by multiple servers. The first non-error response for a
/// segment wins; identical replicas produce the same metadata, so the ordering is stable.
///
/// ### Per-field derivation
/// | Field | Source |
/// |---|---|
/// | `ndv` | `cardinality` (per-segment, from server) |
/// | `minValue`/`maxValue` | JSON strings as-is |
/// | `avgBytesPerValue` | fixed-width: type width; variable: `(shortest+longest)/2`; MV: × avg entries/row |
/// | `minTrusted` | `false` when column is nullable AND `minValue` equals the type's MIN sentinel |
/// | `nullFraction` | `-1` (endpoint does not expose null counts in v1) |
///
/// ### Thread-safety
/// Instances are thread-safe. The [MultiHttpRequest] is constructed per-call and does not
/// hold mutable shared state.
public class BrokerPullColumnStatsSource implements ColumnStatsSource, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerPullColumnStatsSource.class);

  /// Fetch timeout in milliseconds per server request.
  static final int DEFAULT_FETCH_TIMEOUT_MS = 30_000;

  private final ExecutorService _executor;
  private final HttpClientConnectionManager _connectionManager;
  private final ServerSegmentProvider _serverSegmentProvider;
  /// Lazy supplier: the TableCache may not be initialized at construction time (it is set after
  /// the routing manager initializes, which is before this source is constructed). By the time
  /// column-stats pulls fire (always after the debounce interval), the table cache is guaranteed
  /// to be non-null. The supplier may return null; in that case minTrusted defaults to true.
  private final Function<String, TableCache> _tableCacheSupplier;
  private final int _fetchTimeoutMs;

  public BrokerPullColumnStatsSource(ExecutorService executor, HttpClientConnectionManager connectionManager,
      ServerSegmentProvider serverSegmentProvider, Function<String, TableCache> tableCacheSupplier) {
    this(executor, connectionManager, serverSegmentProvider, tableCacheSupplier, DEFAULT_FETCH_TIMEOUT_MS);
  }

  public BrokerPullColumnStatsSource(ExecutorService executor, HttpClientConnectionManager connectionManager,
      ServerSegmentProvider serverSegmentProvider, Function<String, TableCache> tableCacheSupplier,
      int fetchTimeoutMs) {
    _executor = executor;
    _connectionManager = connectionManager;
    _serverSegmentProvider = serverSegmentProvider;
    _tableCacheSupplier = tableCacheSupplier;
    _fetchTimeoutMs = fetchTimeoutMs;
  }

  @Override
  public void close()
      throws IOException {
    _executor.shutdown();
    try {
      _connectionManager.close();
    } catch (Exception e) {
      LOGGER.warn("Error closing HTTP connection manager: {}", e.getMessage());
    }
  }

  @Override
  public Map<String, List<SegmentColumnStatsRow>> fetchColumnStats(String tableNameWithType,
      Set<String> segmentNames)
      throws Exception {
    if (segmentNames.isEmpty()) {
      return Map.of();
    }

    // Resolve server → segments and server → endpoint
    ServerSegmentProvider.Resolution resolution = _serverSegmentProvider.resolve(tableNameWithType, segmentNames);
    Map<String, List<String>> serverToSegments = resolution.serverToSegments();
    Map<String, String> serverToEndpoint = resolution.serverToEndpoint();

    if (serverToSegments.isEmpty()) {
      LOGGER.warn("No servers found for table {} and {} segments; returning empty stats", tableNameWithType,
          segmentNames.size());
      return Map.of();
    }

    // Load schema once for null-sentinel detection. Null schema → minTrusted defaults to true.
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableCache tableCache = _tableCacheSupplier.apply(tableNameWithType);
    Schema schema = tableCache != null ? tableCache.getSchema(rawTableName) : null;

    // Build one URL per server (bulk endpoint with all requested segments and '*' for all columns)
    List<String> urls = new ArrayList<>(serverToSegments.size());
    // Map from URL to server instance ID for logging
    Map<String, String> urlToServer = new HashMap<>(serverToSegments.size());
    for (Map.Entry<String, List<String>> entry : serverToSegments.entrySet()) {
      String server = entry.getKey();
      String endpoint = serverToEndpoint.get(server);
      if (endpoint == null) {
        LOGGER.warn("No admin endpoint for server {}; skipping", server);
        continue;
      }
      String url = buildUrl(endpoint, tableNameWithType, entry.getValue());
      urls.add(url);
      urlToServer.put(url, server);
    }

    if (urls.isEmpty()) {
      return Map.of();
    }

    // Fan out GETs in parallel using MultiHttpRequest (pinot-common)
    CompletionService<MultiHttpRequestResponse> completionService =
        new MultiHttpRequest(_executor, _connectionManager).executeGet(urls, null, _fetchTimeoutMs);

    // Parse responses — accumulate per segment; first win on duplicates (replica dedup)
    Map<String, List<SegmentColumnStatsRow>> result = new HashMap<>();
    for (int i = 0; i < urls.size(); i++) {
      MultiHttpRequestResponse httpResponse = null;
      try {
        httpResponse = completionService.take().get();
        int statusCode = httpResponse.getResponse().getCode();
        String url = httpResponse.getURI().toString();
        if (statusCode >= 300) {
          LOGGER.warn("Server {} returned HTTP {} for column-stats request; skipping",
              urlToServer.getOrDefault(url, url), statusCode);
          continue;
        }
        String body = EntityUtils.toString(httpResponse.getResponse().getEntity());
        JsonNode root = JsonUtils.stringToJsonNode(body);
        // root is Map<segmentName, segmentMetadataJson>
        Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> field = fields.next();
          String segmentName = field.getKey();
          if (result.containsKey(segmentName)) {
            // Already obtained from another replica — skip
            continue;
          }
          JsonNode segNode = field.getValue();
          List<SegmentColumnStatsRow> rows = parseSegmentStats(segmentName, segNode, schema);
          if (!rows.isEmpty()) {
            result.put(segmentName, rows);
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to fetch/parse column stats response for table {}; skipping: {}",
            tableNameWithType, e.getMessage());
      } finally {
        if (httpResponse != null) {
          try {
            httpResponse.close();
          } catch (Exception ignored) {
            // ignore close errors
          }
        }
      }
    }
    return result;
  }

  // ---------------------------------------------------------------------------
  // URL building
  // ---------------------------------------------------------------------------

  private static String buildUrl(String endpoint, String tableNameWithType, List<String> segments) {
    String encodedTable = URLEncoder.encode(tableNameWithType, StandardCharsets.UTF_8);
    StringBuilder sb = new StringBuilder(endpoint)
        .append("/tables/").append(encodedTable)
        .append("/segments/metadata");
    // columns=* and per-segment filters
    sb.append("?columns=*");
    for (String seg : segments) {
      sb.append("&segments=").append(URLEncoder.encode(seg, StandardCharsets.UTF_8));
    }
    return sb.toString();
  }

  // ---------------------------------------------------------------------------
  // Parsing — package-visible for unit testing without a live server
  // ---------------------------------------------------------------------------

  /// Parses a server-response segment JSON node into a list of [SegmentColumnStatsRow]s.
  ///
  /// The `segNode` is the per-segment JSON as produced by
  /// `SegmentMetadataImpl.toJson(columnFilter)`: it has a `totalDocs` field and a `columns`
  /// array where each element is the JSON serialization of `ColumnMetadataImpl`.
  ///
  /// @param segmentName the segment name (key in the outer response map)
  /// @param segNode     the segment's JSON node
  /// @param schema      the table schema used for null-sentinel detection (may be null)
  /// @return list of per-column rows; empty if the JSON is malformed or has no column data
  static List<SegmentColumnStatsRow> parseSegmentStats(String segmentName, JsonNode segNode,
      @Nullable Schema schema) {
    JsonNode columnsNode = segNode.get("columns");
    if (columnsNode == null || !columnsNode.isArray()) {
      return List.of();
    }

    long totalDocs = segNode.has("totalDocs") ? segNode.get("totalDocs").asLong(0) : 0;

    List<SegmentColumnStatsRow> rows = new ArrayList<>();
    for (JsonNode colNode : columnsNode) {
      SegmentColumnStatsRow row = parseColumnNode(segmentName, colNode, totalDocs, schema);
      if (row != null) {
        rows.add(row);
      }
    }
    return rows;
  }

  /// Parses a single column JSON node into a [SegmentColumnStatsRow].
  ///
  /// The column node is the Jackson serialization of `ColumnMetadataImpl` which implements
  /// `ColumnShape`. Key JSON fields: `columnName`, `dataType`, `cardinality`, `totalDocs`,
  /// `totalNumberOfEntries`, `lengthOfShortestElement`, `lengthOfLongestElement`,
  /// `minValue`, `maxValue`, `singleValue`.
  ///
  /// @return the parsed row, or `null` if the column name cannot be determined
  @Nullable
  static SegmentColumnStatsRow parseColumnNode(String segmentName, JsonNode colNode, long totalDocs,
      @Nullable Schema schema) {
    // Column name — Jackson serializes ColumnShape.getColumnName() as "columnName"
    String columnName = extractColumnName(colNode);
    if (columnName == null) {
      return null;
    }

    // NDV
    long ndv = colNode.has("cardinality") ? colNode.get("cardinality").asLong(-1) : -1L;
    if (ndv < 0) {
      ndv = -1;
    }

    // Min / max as strings
    String minValue = colNode.has("minValue") && !colNode.get("minValue").isNull()
        ? colNode.get("minValue").asText() : null;
    String maxValue = colNode.has("maxValue") && !colNode.get("maxValue").isNull()
        ? colNode.get("maxValue").asText() : null;

    // Data type for avgBytesPerValue and minTrusted logic
    DataType storedType = extractStoredType(colNode);

    // singleValue flag
    boolean singleValue = !colNode.has("singleValue") || colNode.get("singleValue").asBoolean(true);

    // totalNumberOfEntries (multi-value total)
    long totalEntries = colNode.has("totalNumberOfEntries")
        ? colNode.get("totalNumberOfEntries").asLong(totalDocs) : totalDocs;

    // avgBytesPerValue
    double avgBytesPerValue = computeAvgBytes(storedType, singleValue, totalDocs, totalEntries, colNode);

    // minTrusted
    boolean minTrusted = computeMinTrusted(columnName, minValue, storedType, schema);

    // nullFraction — v1: unknown
    double nullFraction = -1.0;

    return new SegmentColumnStatsRow(segmentName, columnName, ndv, minValue, maxValue, minTrusted,
        avgBytesPerValue, nullFraction);
  }

  // ---------------------------------------------------------------------------
  // Field derivation helpers
  // ---------------------------------------------------------------------------

  @Nullable
  private static String extractColumnName(JsonNode colNode) {
    // ColumnShape.getColumnName() is serialized by Jackson as "columnName"
    if (colNode.has("columnName")) {
      return colNode.get("columnName").asText(null);
    }
    // Fallback: nested fieldSpec object with "name"
    JsonNode fs = colNode.get("fieldSpec");
    if (fs != null && fs.has("name")) {
      return fs.get("name").asText(null);
    }
    return null;
  }

  @Nullable
  private static DataType extractStoredType(JsonNode colNode) {
    // ColumnShape.getDataType() returns the stored type and is serialized by Jackson as "dataType"
    if (colNode.has("dataType")) {
      try {
        return DataType.valueOf(colNode.get("dataType").asText().toUpperCase());
      } catch (IllegalArgumentException ignored) {
        // fall through
      }
    }
    // Fallback: nested fieldSpec.dataType (before stored-type conversion)
    JsonNode fs = colNode.get("fieldSpec");
    if (fs != null && fs.has("dataType")) {
      try {
        DataType raw = DataType.valueOf(fs.get("dataType").asText().toUpperCase());
        return raw.getStoredType();
      } catch (IllegalArgumentException ignored) {
        // fall through
      }
    }
    return null;
  }

  /// Computes avg bytes per stored value using the column's stored type and shape statistics.
  ///
  /// For fixed-width stored types the byte width is used directly. For variable-width types
  /// (STRING, BYTES, BIG_DECIMAL) the average is estimated from the shortest and longest
  /// element lengths, falling back to the longest element, then to [FieldSpec#DEFAULT_MAX_LENGTH].
  /// For multi-value columns the per-entry size is multiplied by the average number of entries
  /// per row.
  static double computeAvgBytes(@Nullable DataType storedType, boolean singleValue, long totalDocs,
      long totalEntries, JsonNode colNode) {
    double bytesPerEntry = fixedWidthBytes(storedType);
    if (bytesPerEntry < 0) {
      // Variable-width — estimate from element lengths
      int shortest = colNode.has("lengthOfShortestElement")
          ? colNode.get("lengthOfShortestElement").asInt(-1) : -1;
      int longest = colNode.has("lengthOfLongestElement")
          ? colNode.get("lengthOfLongestElement").asInt(-1) : -1;
      if (shortest >= 0 && longest >= 0) {
        bytesPerEntry = (shortest + longest) / 2.0;
      } else if (longest >= 0) {
        bytesPerEntry = longest;
      } else {
        bytesPerEntry = FieldSpec.DEFAULT_MAX_LENGTH;
      }
    }

    if (!singleValue && totalDocs > 0) {
      double avgEntriesPerRow = (double) totalEntries / totalDocs;
      return bytesPerEntry * avgEntriesPerRow;
    }
    return bytesPerEntry;
  }

  /// Returns the fixed byte width for a stored type, or `-1` for variable-width types.
  static double fixedWidthBytes(@Nullable DataType storedType) {
    if (storedType == null) {
      return -1;
    }
    switch (storedType) {
      case INT:
        return 4;
      case LONG:
        return 8;
      case FLOAT:
        return 4;
      case DOUBLE:
        return 8;
      case BOOLEAN:
        return 1;
      case TIMESTAMP:
        return 8;
      default:
        return -1; // STRING, BYTES, BIG_DECIMAL are variable-width
    }
  }

  /// Returns `true` when the minimum value is trustworthy for segment pruning.
  ///
  /// `minTrusted` is `false` only when:
  /// 1. The column is nullable (as reported by the schema), AND
  /// 2. The stored min equals the type's minimum representable value (the null-sentinel default).
  ///
  /// See memory note: Nulls skew Pinot min/max metadata.
  static boolean computeMinTrusted(String columnName, @Nullable String minValue, @Nullable DataType storedType,
      @Nullable Schema schema) {
    if (minValue == null || storedType == null) {
      return true;
    }
    // Check if the column is nullable according to the schema
    boolean nullable = false;
    if (schema != null) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(columnName);
      if (fieldSpec != null) {
        nullable = fieldSpec.isNullable();
      }
    }
    if (!nullable) {
      return true;
    }
    // Column is nullable — check if minValue is the null-sentinel
    return !isNullSentinel(storedType, minValue);
  }

  /// Returns `true` when `valueStr` matches the null-sentinel for the given stored type.
  static boolean isNullSentinel(DataType storedType, String valueStr) {
    try {
      switch (storedType) {
        case INT:
          return Integer.parseInt(valueStr) == Integer.MIN_VALUE;
        case LONG:
          return Long.parseLong(valueStr) == Long.MIN_VALUE;
        case FLOAT:
          return Float.parseFloat(valueStr) == -Float.MAX_VALUE;
        case DOUBLE:
          return Double.parseDouble(valueStr) == -Double.MAX_VALUE;
        default:
          return false;
      }
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
