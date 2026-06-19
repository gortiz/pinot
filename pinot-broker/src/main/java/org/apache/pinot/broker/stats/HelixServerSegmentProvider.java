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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Production [ServerSegmentProvider] implementation that resolves server→segments from the
/// Helix ExternalView stored in ZooKeeper, and derives server admin endpoints from the broker's
/// live ServerInstance map.
///
/// ### Design rationale
/// The broker already maintains an up-to-date `serverInstanceId → ServerInstance` map (updated
/// on Helix instance-config-change events) where each [ServerInstance] knows its HTTP admin
/// endpoint. Rather than issuing a separate ZK lookup per call, we read the ExternalView once
/// per `resolve()` call (it is small and ZK-cached), combine it with the in-memory instance map,
/// and build the server→segments and server→endpoint maps in one pass.
///
/// Segments in ERROR or OFFLINE state in the ExternalView are skipped; only ONLINE and CONSUMING
/// segments are included.
///
/// ### Thread-safety
/// Thread-safe: both the `serverInstanceMap` supplier and the ZK property store lookups are
/// thread-safe. The `enabledServers` snapshot taken during each call is consistent within that
/// call.
public class HelixServerSegmentProvider implements ServerSegmentProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixServerSegmentProvider.class);

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _externalViewPathPrefix;
  private final Map<String, ServerInstance> _enabledServerInstanceMap;

  /// Creates a new provider backed by ZK and the broker's live server-instance map.
  ///
  /// @param propertyStore          ZK property store used to read ExternalViews
  /// @param externalViewPathPrefix ZK path prefix for ExternalViews (e.g. `/CLUSTER/EXTERNALVIEW/`)
  /// @param enabledServerInstanceMap live map from server instance ID to [ServerInstance]; the
  ///                                  caller passes a reference to the routing manager's map —
  ///                                  the map is read on every `resolve()` call so it stays current
  public HelixServerSegmentProvider(ZkHelixPropertyStore<ZNRecord> propertyStore, String externalViewPathPrefix,
      Map<String, ServerInstance> enabledServerInstanceMap) {
    _propertyStore = propertyStore;
    _externalViewPathPrefix = externalViewPathPrefix;
    _enabledServerInstanceMap = enabledServerInstanceMap;
  }

  @Override
  public Resolution resolve(String tableNameWithType, Set<String> segmentNames) {
    ZNRecord znRecord = _propertyStore.get(_externalViewPathPrefix + tableNameWithType, null,
        AccessOption.PERSISTENT);
    if (znRecord == null) {
      LOGGER.warn("No ExternalView for table {}; cannot resolve server segments", tableNameWithType);
      return new Resolution(Map.of(), Map.of());
    }

    ExternalView externalView = new ExternalView(znRecord);
    Map<String, Map<String, String>> assignment = externalView.getRecord().getMapFields();

    // server → list of requested segments it hosts
    Map<String, List<String>> serverToSegments = new HashMap<>();
    for (String segment : segmentNames) {
      Map<String, String> serverStateMap = assignment.get(segment);
      if (serverStateMap == null) {
        continue;
      }
      // Pick only servers in ONLINE or CONSUMING state; avoid ERROR/OFFLINE
      for (Map.Entry<String, String> serverState : serverStateMap.entrySet()) {
        String state = serverState.getValue();
        if (SegmentStateModel.ONLINE.equals(state) || SegmentStateModel.CONSUMING.equals(state)) {
          serverToSegments.computeIfAbsent(serverState.getKey(), k -> new ArrayList<>()).add(segment);
          // Take first qualifying server only — the caller deduplicates by segment name on merge
          break;
        }
      }
    }

    // Build server → endpoint from the live instance map
    Map<String, String> serverToEndpoint = new HashMap<>(serverToSegments.size());
    for (String serverId : serverToSegments.keySet()) {
      ServerInstance instance = _enabledServerInstanceMap.get(serverId);
      if (instance == null) {
        LOGGER.debug("Server {} has segments in ExternalView but is not in enabled instance map; skipping", serverId);
        continue;
      }
      String endpoint = instance.getAdminEndpoint();
      if (endpoint == null) {
        LOGGER.warn("Server {} has no admin endpoint configured; skipping", serverId);
        continue;
      }
      serverToEndpoint.put(serverId, endpoint);
    }

    // Remove servers without a resolvable endpoint
    serverToSegments.keySet().retainAll(serverToEndpoint.keySet());

    return new Resolution(serverToSegments, serverToEndpoint);
  }
}
