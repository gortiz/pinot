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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetchListener;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Broker-wide singleton that owns one [StatsStore] and produces per-table
/// [SegmentZkMetadataFetchListener] instances that populate it.
///
/// ### Usage
/// 1. Construct with a pre-created (but not yet init()d) [StatsStore], and optionally a
///    [ColumnStatsSource] for column-level statistics.
/// 1. Call [#init()] — on failure the manager disables itself; broker startup is not affected.
/// 1. For each table, call [#createListener(String)] and register the result on that
///    table's [org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher]
///    *before* the fetcher's own `init()` call.
/// 1. On table removal, call [#onTableRemoved(String)].
/// 1. Close the manager when the broker shuts down.
///
/// ### Column-stats background pull
/// When a [ColumnStatsSource] is provided, the manager asynchronously fetches column statistics
/// for segments added or changed since the last successful fetch (CRC-delta). Requests are
/// debounced: changed segments are collected and dispatched as one batched call per table at
/// most once every `refreshIntervalSec` seconds. A single dedicated
/// [ScheduledExecutorService] thread handles all tables to bound total concurrency and avoid
/// surprising query-path latency spikes.
///
/// ### Load math
/// Each fetch is one bulk HTTP GET per server per table. With T tables, S servers per table,
/// and a refresh interval of R seconds the steady-state rate is at most T × S / R GETs/second.
/// For a typical deployment (100 tables, 5 servers, R = 300 s) that is ≈1.7 GETs/s — well
/// within what a Pinot server can absorb. Refreshes are not coordinated across brokers, so N
/// brokers add an N× multiplier; the default R = 300 s keeps this acceptable even at N = 10.
///
/// ### Thread-safety
/// Read methods ([#getTableStats], [#estimateRowsInTimeRange]) are safe for
/// concurrent access by any number of reader threads. [#createListener] and
/// [#onTableRemoved] are called from the routing-manager's table-build thread; the store
/// itself handles single-writer / multi-reader concurrency internally. The column-stats
/// background thread accesses the store sequentially — only one pull is in flight at a time
/// per table, enforced by the single-thread executor.
///
/// ### Failure isolation
/// All [StatsStoreException] escapes are suppressed here — callers on the query path
/// will receive `null` / empty rather than a propagated exception. Column-stats fetch failures
/// are logged and discarded; they never propagate to the listener or the query path.
public class BrokerTableStatsManager implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerTableStatsManager.class);

  private final StatsStore _statsStore;
  private final LogicalTableStatsResolver _resolver;
  @Nullable
  private final ColumnStatsSource _columnStatsSource;
  private final long _columnRefreshIntervalMs;
  /// Single-thread executor for all column-stats background pulls. Null when no column source.
  @Nullable
  private final ScheduledExecutorService _columnStatsExecutor;

  /// False when init() failed; all operations become no-ops in that state.
  private volatile boolean _enabled = false;

  /// Constructs a new manager backed by the given [StatsStore] with no column-stats source.
  /// The store must not have been opened yet; [#init()] will call [StatsStore#init()].
  ///
  /// @param statsStore backing store; owned by this manager
  public BrokerTableStatsManager(StatsStore statsStore) {
    this(statsStore, null, 0);
  }

  /// Constructs a new manager with an optional [ColumnStatsSource] for column-level statistics.
  ///
  /// @param statsStore             backing store; owned by this manager
  /// @param columnStatsSource      source to pull column stats from; null disables column pulls
  /// @param columnRefreshIntervalMs debounce window: segments are coalesced and fetched in one
  ///                                batch per table at most once per this interval (milliseconds)
  public BrokerTableStatsManager(StatsStore statsStore, @Nullable ColumnStatsSource columnStatsSource,
      long columnRefreshIntervalMs) {
    _statsStore = statsStore;
    _resolver = new LogicalTableStatsResolver(statsStore);
    _columnStatsSource = columnStatsSource;
    _columnRefreshIntervalMs = columnRefreshIntervalMs > 0 ? columnRefreshIntervalMs : 0;
    _columnStatsExecutor = columnStatsSource != null
        ? Executors.newSingleThreadScheduledExecutor(r -> {
          Thread t = new Thread(r, "broker-column-stats-puller");
          t.setDaemon(true);
          return t;
        }) : null;
  }

  /// Sets the provider used to look up the time boundary (epoch-milliseconds) for a raw table
  /// name. Call this after the routing manager is fully initialized.
  ///
  /// A `null` return value from `provider` means no boundary is available for
  /// that table, which causes the resolver to fall back to a plain sum of offline + realtime rows
  /// with [org.apache.pinot.query.planner.spi.stats.StatConfidence#ESTIMATED] confidence.
  ///
  /// @param provider function from raw table name → time boundary in epoch-milliseconds, nullable
  public void setTimeBoundaryMsProvider(@Nullable Function<String, Long> provider) {
    _resolver.setTimeBoundaryMsProvider(provider);
  }

  /// Sets the provider used to look up the [TableConfig] for a fully-qualified
  /// (type-suffixed) table name. Call this after the table cache is initialized.
  ///
  /// Required for upsert/dedup detection; without this, upsert/dedup tables will report
  /// [org.apache.pinot.query.planner.spi.stats.StatConfidence#EXACT] rather than
  /// [org.apache.pinot.query.planner.spi.stats.StatConfidence#LOW].
  ///
  /// @param provider function from suffixed table name → TableConfig, nullable
  public void setTableConfigProvider(@Nullable Function<String, TableConfig> provider) {
    _resolver.setTableConfigProvider(provider);
  }

  /// Opens the backing store. On failure, logs an error and sets the manager to disabled; the
  /// broker should still start normally.
  ///
  /// @throws StatsStoreException if the store cannot be opened (callers may log and ignore)
  public void init()
      throws StatsStoreException {
    _statsStore.init();
    _enabled = true;
    LOGGER.info("BrokerTableStatsManager initialized");
  }

  /// Creates a listener that will maintain stats for `tableNameWithType` in the backing
  /// store. Must be registered on the table's
  /// [org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher] before the
  /// fetcher is initialized.
  ///
  /// If the manager is disabled (init failed), returns a no-op listener.
  ///
  /// @param tableNameWithType fully-qualified table name (e.g. `myTable_OFFLINE`)
  /// @return a new listener instance for that table
  public SegmentZkMetadataFetchListener createListener(String tableNameWithType) {
    if (!_enabled) {
      return NoOpListener.INSTANCE;
    }
    return new TableStatsZkListener(tableNameWithType, _statsStore, _columnStatsSource, _columnStatsExecutor,
        _columnRefreshIntervalMs);
  }

  /// Removes all persisted stats for the given table. Called when the routing entry for a table
  /// is removed. Any store error is logged at WARN and ignored.
  ///
  /// @param tableNameWithType fully-qualified table name
  public void onTableRemoved(String tableNameWithType) {
    if (!_enabled) {
      return;
    }
    try {
      _statsStore.purgeTable(tableNameWithType);
    } catch (StatsStoreException e) {
      LOGGER.warn("Failed to purge stats for table {}: {}", tableNameWithType, e.getMessage());
    }
  }

  /// Returns logical table statistics for the given table name, or `null` if unavailable.
  ///
  /// Accepts both suffixed physical names (`foo_OFFLINE` / `foo_REALTIME`) and raw
  /// logical names (`foo`):
  /// - Suffixed names: returns physical stats with per-type confidence adjustments (upsert,
  ///   dedup, consuming-segment detection).
  /// - Raw names: returns a logical hybrid view merging offline and realtime stats at the
  ///   time boundary; if no boundary is available, returns a plain sum with
  ///   [org.apache.pinot.query.planner.spi.stats.StatConfidence#ESTIMATED] confidence.
  ///
  /// Any store error is logged at WARN and `null` is returned.
  ///
  /// @param tableName raw table name or fully-qualified name with type suffix
  @Nullable
  public TableStatistics getTableStats(String tableName) {
    if (!_enabled) {
      return null;
    }
    return _resolver.getTableStats(tableName);
  }

  /// Returns an estimate of the number of rows in the given time range, or an empty optional if
  /// unavailable. Any store error is logged at WARN and an empty optional is returned.
  ///
  /// For hybrid (raw) table names, the estimate is split at the time boundary:
  /// offline rows are counted for `[startMs, boundary)` and realtime rows for
  /// `[boundary, endMs)`.
  ///
  /// @param tableName raw table name or fully-qualified name with type suffix
  /// @param startMs   inclusive range start in epoch milliseconds
  /// @param endMs     exclusive range end in epoch milliseconds
  public OptionalLong estimateRowsInTimeRange(String tableName, long startMs, long endMs) {
    if (!_enabled) {
      return OptionalLong.empty();
    }
    return _resolver.estimateRowsInTimeRange(tableName, startMs, endMs);
  }

  @Override
  public void close()
      throws IOException {
    // Disable before closing the store so that concurrent read calls on the query path
    // short-circuit cleanly without triggering WARN log spam from a closed store.
    _enabled = false;
    if (_columnStatsExecutor != null) {
      _columnStatsExecutor.shutdownNow();
    }
    if (_columnStatsSource instanceof Closeable) {
      try {
        ((Closeable) _columnStatsSource).close();
      } catch (IOException e) {
        LOGGER.warn("Error closing ColumnStatsSource: {}", e.getMessage());
      }
    }
    try {
      _statsStore.close();
    } catch (IOException e) {
      LOGGER.warn("Error closing StatsStore: {}", e.getMessage());
      throw e;
    }
  }

  // ---------------------------------------------------------------------------
  // Inner class: TableStatsZkListener
  // ---------------------------------------------------------------------------

  /// [SegmentZkMetadataFetchListener] that maintains segment-level statistics for a single
  /// table in a [StatsStore].
  ///
  /// ### Column-stats background pull
  /// When a [ColumnStatsSource] and [ScheduledExecutorService] are provided, segment changes
  /// are accumulated in `_pendingColumnSegments` (under the routing-manager's per-table lock)
  /// and a debounced fetch task is scheduled on the background executor. The task fires after
  /// `refreshIntervalMs` from the first change in the current burst, then clears the pending
  /// set and runs the fan-out. A subsequent change while a task is pending simply adds the
  /// segment to the set — no new task is scheduled — so bursts coalesce into one fetch.
  ///
  /// ### Thread-safety
  /// Instances are called sequentially from the routing manager's per-table lock, so the
  /// `_persistedSegments` and `_pendingColumnSegments` sets need no additional synchronization.
  /// The background executor accesses `_statsStore` from a separate thread; [StatsStore] is
  /// documented as single-writer/multi-reader safe, so concurrent upserts from two different
  /// threads would be unsafe — but the single-thread executor ensures at most one column-pull
  /// task runs at a time for any given table.
  ///
  /// ### Failure isolation
  /// All [StatsStoreException] are caught; errors are logged at WARN and the listener
  /// never throws back into the routing manager. Column-stats fetch errors are logged and
  /// dropped; they never affect the segment-stats path.
  static final class TableStatsZkListener implements SegmentZkMetadataFetchListener {
    private static final Logger LOG = LoggerFactory.getLogger(TableStatsZkListener.class);

    private final String _tableNameWithType;
    private final StatsStore _statsStore;
    @Nullable
    private final ColumnStatsSource _columnStatsSource;
    @Nullable
    private final ScheduledExecutorService _columnStatsExecutor;
    private final long _columnRefreshIntervalMs;

    /// In-memory mirror of the segments currently persisted in the store for this table.
    /// Maintained after [#init] so that [#onAssignmentChange] can compute
    /// removals without a full DB round-trip.
    ///
    /// Accessed only from the routing-manager's per-table lock — no additional
    /// synchronization needed.
    private final Set<String> _persistedSegments = new HashSet<>();

    /// Segments queued for the next column-stats fetch (changed or new since the last fetch).
    /// Cleared when the debounced task fires. Written from the routing-manager's per-table lock
    /// (via [#scheduleColumnFetch]); drained from the background executor thread (via
    /// [#runColumnFetch]). Uses a thread-safe set so that concurrent access is safe.
    private final Set<String> _pendingColumnSegments = ConcurrentHashMap.newKeySet();

    /// Reference to the currently scheduled (not yet fired) debounce task, or null.
    ///
    /// Written from two threads:
    /// - the routing-manager's per-table lock thread (in [#scheduleColumnFetch]) which sets it
    ///   to a new [ScheduledFuture]; and
    /// - the background executor thread (in [#runColumnFetch]) which resets it to null.
    ///
    /// Declared `volatile` so both threads see a consistent value without requiring a shared lock.
    @Nullable
    private volatile ScheduledFuture<?> _pendingColumnFetch;

    TableStatsZkListener(String tableNameWithType, StatsStore statsStore) {
      this(tableNameWithType, statsStore, null, null, 0);
    }

    TableStatsZkListener(String tableNameWithType, StatsStore statsStore,
        @Nullable ColumnStatsSource columnStatsSource,
        @Nullable ScheduledExecutorService columnStatsExecutor, long columnRefreshIntervalMs) {
      _tableNameWithType = tableNameWithType;
      _statsStore = statsStore;
      _columnStatsSource = columnStatsSource;
      _columnStatsExecutor = columnStatsExecutor;
      _columnRefreshIntervalMs = columnRefreshIntervalMs;
    }

    @Override
    public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
        List<ZNRecord> znRecords) {
      // Restart reconciliation: read stored CRCs, upsert changed/new, remove dropped segments.
      Map<String, Long> storedCrcs;
      try {
        storedCrcs = _statsStore.getSegmentCrcs(_tableNameWithType);
      } catch (StatsStoreException e) {
        LOG.warn("Failed to read stored CRCs for {} during init; will upsert all segments: {}",
            _tableNameWithType, e.getMessage());
        storedCrcs = Map.of();
      }

      int n = onlineSegments.size();
      List<SegmentStatsRow> toUpsert = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        String segment = onlineSegments.get(i);
        ZNRecord znRecord = znRecords.get(i);
        if (znRecord == null) {
          continue;
        }
        SegmentZKMetadata meta = new SegmentZKMetadata(znRecord);
        long crc = meta.getCrc();
        Long stored = storedCrcs.get(segment);
        if (stored != null && stored == crc) {
          // CRC matches — row stats data is still valid, skip upsert but track as persisted.
          // Still schedule a column-stats fetch: the column-stats source may have been enabled for
          // the first time after this broker restart (CRC-delta only guards row stats, not column
          // stats). The debounce window coalesces all segments into one bulk fetch.
          _persistedSegments.add(segment);
          scheduleColumnFetch(segment);
          continue;
        }
        toUpsert.add(buildRow(segment, meta));
      }

      if (!toUpsert.isEmpty()) {
        try {
          _statsStore.upsertSegmentStats(_tableNameWithType, toUpsert);
          for (SegmentStatsRow row : toUpsert) {
            _persistedSegments.add(row.getSegmentName());
          }
        } catch (StatsStoreException e) {
          LOG.warn("Failed to upsert segment stats for {} during init: {}", _tableNameWithType,
              e.getMessage());
        }
        // Schedule a column-stats fetch for changed/new segments
        for (SegmentStatsRow row : toUpsert) {
          scheduleColumnFetch(row.getSegmentName());
        }
      }

      // Remove persisted segments that are no longer online
      Set<String> onlineSet = Set.copyOf(onlineSegments);
      List<String> toRemove = new ArrayList<>();
      for (String persisted : storedCrcs.keySet()) {
        if (!onlineSet.contains(persisted)) {
          toRemove.add(persisted);
        }
      }
      if (!toRemove.isEmpty()) {
        try {
          _statsStore.removeSegments(_tableNameWithType, toRemove);
          _persistedSegments.removeAll(toRemove);
        } catch (StatsStoreException e) {
          LOG.warn("Failed to remove stale segments for {} during init: {}", _tableNameWithType,
              e.getMessage());
        }
        _pendingColumnSegments.removeAll(toRemove);
      }
    }

    @Override
    public void onAssignmentChange(IdealState idealState, ExternalView externalView,
        Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
      // Upsert newly-online segments
      int n = pulledSegments.size();
      if (n > 0) {
        List<SegmentStatsRow> toUpsert = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
          ZNRecord znRecord = znRecords.get(i);
          if (znRecord == null) {
            continue;
          }
          SegmentZKMetadata meta = new SegmentZKMetadata(znRecord);
          toUpsert.add(buildRow(pulledSegments.get(i), meta));
        }
        if (!toUpsert.isEmpty()) {
          try {
            _statsStore.upsertSegmentStats(_tableNameWithType, toUpsert);
            for (SegmentStatsRow row : toUpsert) {
              _persistedSegments.add(row.getSegmentName());
            }
          } catch (StatsStoreException e) {
            LOG.warn("Failed to upsert segment stats for {} on assignment change: {}",
                _tableNameWithType, e.getMessage());
          }
          for (SegmentStatsRow row : toUpsert) {
            scheduleColumnFetch(row.getSegmentName());
          }
        }
      }

      // Remove persisted segments that are no longer online.
      // Use the in-memory mirror to avoid a full DB round-trip.
      List<String> toRemove = new ArrayList<>();
      for (String persisted : _persistedSegments) {
        if (!onlineSegments.contains(persisted)) {
          toRemove.add(persisted);
        }
      }
      if (!toRemove.isEmpty()) {
        try {
          _statsStore.removeSegments(_tableNameWithType, toRemove);
          _persistedSegments.removeAll(toRemove);
        } catch (StatsStoreException e) {
          LOG.warn("Failed to remove dropped segments for {} on assignment change: {}",
              _tableNameWithType, e.getMessage());
        }
        _pendingColumnSegments.removeAll(toRemove);
      }
    }

    @Override
    public void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
      if (znRecord == null) {
        // Segment disappeared — remove from store
        try {
          _statsStore.removeSegments(_tableNameWithType, List.of(segment));
          _persistedSegments.remove(segment);
        } catch (StatsStoreException e) {
          LOG.warn("Failed to remove segment {} for {} on refresh: {}", segment,
              _tableNameWithType, e.getMessage());
        }
        _pendingColumnSegments.remove(segment);
        return;
      }
      SegmentZKMetadata meta = new SegmentZKMetadata(znRecord);
      List<SegmentStatsRow> rows = List.of(buildRow(segment, meta));
      try {
        _statsStore.upsertSegmentStats(_tableNameWithType, rows);
        _persistedSegments.add(segment);
      } catch (StatsStoreException e) {
        LOG.warn("Failed to upsert segment {} for {} on refresh: {}", segment, _tableNameWithType,
            e.getMessage());
      }
      scheduleColumnFetch(segment);
    }

    // ---------------------------------------------------------------------------
    // Column-stats debounce helpers
    // ---------------------------------------------------------------------------

    /// Adds `segment` to the pending set and, if no task is already scheduled, schedules a
    /// debounced fetch task. Called only from the routing-manager's per-table lock.
    private void scheduleColumnFetch(String segment) {
      if (_columnStatsSource == null || _columnStatsExecutor == null) {
        return;
      }
      _pendingColumnSegments.add(segment);
      if (_pendingColumnFetch == null || _pendingColumnFetch.isDone()) {
        // Snapshot table name and executor reference before scheduling
        String table = _tableNameWithType;
        _pendingColumnFetch = _columnStatsExecutor.schedule(() -> runColumnFetch(table),
            _columnRefreshIntervalMs, TimeUnit.MILLISECONDS);
      }
      // else: a task is already pending — the segment is added to _pendingColumnSegments and will
      // be picked up when the existing task fires.
    }

    /// Drains `_pendingColumnSegments`, fetches column stats for them, and persists the result.
    /// Called on the background executor thread — never on the ZK-listener thread.
    ///
    /// The pending set is thread-safe ([ConcurrentHashMap.newKeySet()]); we snapshot and clear it
    /// atomically-enough: any segments added after the snapshot is taken will be picked up on the
    /// next scheduled run. Since `_pendingColumnFetch` is set to null before the fetch runs, a
    /// concurrent [#scheduleColumnFetch] call will schedule a new task if new segments arrive
    /// while a fetch is in progress.
    private void runColumnFetch(String table) {
      // Clear the pending fetch reference first so new changes schedule a fresh task
      _pendingColumnFetch = null;

      if (_pendingColumnSegments.isEmpty()) {
        return;
      }
      // Snapshot and drain — any new segments added concurrently will be in the next run
      Set<String> toFetch = Set.copyOf(_pendingColumnSegments);
      _pendingColumnSegments.removeAll(toFetch);

      try {
        Map<String, List<SegmentColumnStatsRow>> fetched = _columnStatsSource.fetchColumnStats(table, toFetch);
        if (fetched.isEmpty()) {
          return;
        }
        List<SegmentColumnStatsRow> allRows = new ArrayList<>();
        for (List<SegmentColumnStatsRow> rows : fetched.values()) {
          allRows.addAll(rows);
        }
        _statsStore.upsertSegmentColumnStats(table, allRows);
        LOG.debug("Persisted column stats for {} segments of table {}", fetched.size(), table);
      } catch (StatsStoreException e) {
        LOG.warn("Failed to persist column stats for table {}: {}", table, e.getMessage());
      } catch (Exception e) {
        LOG.warn("Failed to fetch column stats for table {}: {}", table, e.getMessage());
      }
    }

    /// Converts a [SegmentZKMetadata] into a [SegmentStatsRow].
    /// A segment is considered consuming when it is a realtime segment whose status is
    /// [Status#IN_PROGRESS].
    /// For non-consuming segments, negative totalDocs is stored as 0.
    private static SegmentStatsRow buildRow(String segmentName, SegmentZKMetadata meta) {
      boolean consuming = meta.getStatus() == Status.IN_PROGRESS;
      long totalDocs = meta.getTotalDocs();
      if (!consuming && totalDocs < 0) {
        LOG.debug("Segment {} has negative totalDocs ({}); storing 0", segmentName, totalDocs);
        totalDocs = 0;
      }
      // Size is -1 when unknown in ZK metadata; clamp so SUM(size_bytes) is not skewed downwards
      // by sentinel values.
      long sizeBytes = Math.max(meta.getSizeInBytes(), 0);
      return new SegmentStatsRow(segmentName, meta.getCrc(), totalDocs, sizeBytes,
          meta.getStartTimeMs(), meta.getEndTimeMs(), consuming);
    }
  }

  // ---------------------------------------------------------------------------
  // Inner class: NoOpListener
  // ---------------------------------------------------------------------------

  /// No-op listener returned when the manager is disabled.
  private static final class NoOpListener implements SegmentZkMetadataFetchListener {
    static final NoOpListener INSTANCE = new NoOpListener();

    private NoOpListener() {
    }

    @Override
    public void init(IdealState idealState, ExternalView externalView, List<String> onlineSegments,
        List<ZNRecord> znRecords) {
    }

    @Override
    public void onAssignmentChange(IdealState idealState, ExternalView externalView,
        Set<String> onlineSegments, List<String> pulledSegments, List<ZNRecord> znRecords) {
    }

    @Override
    public void refreshSegment(String segment, @Nullable ZNRecord znRecord) {
    }
  }
}
