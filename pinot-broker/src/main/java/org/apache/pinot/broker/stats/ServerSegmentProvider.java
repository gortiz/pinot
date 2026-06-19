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

import java.util.List;
import java.util.Map;
import java.util.Set;


/// Resolves the set of servers hosting the requested segments for a given table, along with
/// the HTTP admin endpoint for each server.
///
/// The result map keys are server instance IDs (e.g. `Server_10.0.0.1_7500`); each value is
/// the list of requested segments the server hosts. The companion endpoint map is keyed by
/// the same server instance IDs and values are HTTP admin base URLs (e.g.
/// `http://10.0.0.1:7900`).
///
/// Implementations may deduplicate replicas (returning only one server per segment) or return
/// all replicas — [BrokerPullColumnStatsSource] will de-duplicate per segment when merging
/// responses from multiple servers.
///
/// Thread-safety: implementations must be thread-safe.
public interface ServerSegmentProvider {

  /// Result of a server-segment resolution.
  ///
  /// @param serverToSegments map from server instance ID to the requested segments that server hosts
  /// @param serverToEndpoint map from server instance ID to admin endpoint base URL
  record Resolution(Map<String, List<String>> serverToSegments, Map<String, String> serverToEndpoint) {
  }

  /// Returns the server-to-segments assignment for `segmentNames` on `tableNameWithType`.
  ///
  /// Segments not hosted by any known server are omitted from the result.
  ///
  /// @param tableNameWithType fully-qualified table name with type suffix
  /// @param segmentNames      segments for which a server must be resolved
  /// @return resolution containing server-to-segments and server-to-endpoint maps
  Resolution resolve(String tableNameWithType, Set<String> segmentNames);
}
