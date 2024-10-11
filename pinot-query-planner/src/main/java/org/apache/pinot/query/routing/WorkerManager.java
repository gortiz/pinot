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
package org.apache.pinot.query.routing;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.rules.ImmutableTableOptions;
import org.apache.pinot.calcite.rel.rules.TableOptions;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanContext;
import org.apache.pinot.query.planner.physical.DispatchablePlanMetadata;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code WorkerManager} manages stage to worker assignment.
 *
 * <p>It contains the logic to assign worker to a particular stages. If it is a leaf stage the logic fallback to
 * how Pinot server assigned server and server-segment mapping.
 */
public class WorkerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerManager.class);
  private static final Random RANDOM = new Random();
  // default shuffle method in v2
  private static final String DEFAULT_SHUFFLE_PARTITION_FUNCTION = "AbsHashCodeSum";
  // default table partition function if not specified in hint
  private static final String DEFAULT_TABLE_PARTITION_FUNCTION = "Murmur";

  private final String _hostName;
  private final int _port;
  private final RoutingManager _routingManager;

  public WorkerManager(String hostName, int port, RoutingManager routingManager) {
    _hostName = hostName;
    _port = port;
    _routingManager = routingManager;
  }

  @Nullable
  public TablePartitionInfo getTablePartitionInfo(String tableNameWithType) {
    return _routingManager.getTablePartitionInfo(tableNameWithType);
  }

  public void assignWorkers(PlanFragment rootFragment, DispatchablePlanContext context) {
    // ROOT stage doesn't have a QueryServer as it is strictly only reducing results, so here we simply assign the
    // worker instance with identical server/mailbox port number.
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(0);
    metadata.setWorkerIdToServerInstanceMap(
        Collections.singletonMap(0, new QueryServerInstance(_hostName, _port, _port)));
    for (PlanFragment child : rootFragment.getChildren()) {
      assignWorkersToNonRootFragment(child, context);
    }
  }

  private void assignWorkersToNonRootFragment(PlanFragment fragment, DispatchablePlanContext context) {
    List<PlanFragment> children = fragment.getChildren();
    for (PlanFragment child : children) {
      assignWorkersToNonRootFragment(child, context);
    }
    Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
    DispatchablePlanMetadata metadata = metadataMap.get(fragment.getFragmentId());
    boolean leafPlan = isLeafPlan(metadata);
    if (isLocalExchange(children)) {
      // If it is a local exchange (single child with SINGLETON distribution), use the same worker assignment to avoid
      // shuffling data.
      // TODO: Support partition parallelism
      DispatchablePlanMetadata childMetadata = metadataMap.get(children.get(0).getFragmentId());
      metadata.setWorkerIdToServerInstanceMap(childMetadata.getWorkerIdToServerInstanceMap());
      metadata.setPartitionFunction(childMetadata.getPartitionFunction());
      if (leafPlan) {
        // Fake segments map for leaf plan
        Set<Integer> workerIds = metadata.getWorkerIdToServerInstanceMap().keySet();
        Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap =
            Maps.newHashMapWithExpectedSize(workerIds.size());
        for (Integer workerId : workerIds) {
          workerIdToSegmentsMap.put(workerId, Map.of(TableType.OFFLINE.name(), List.of()));
        }
        metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
      }
    } else if (leafPlan) {
      assignWorkersToLeafFragment(fragment, context);
    } else {
      assignWorkersToIntermediateFragment(fragment, context);
    }
  }

  private boolean isLocalExchange(List<PlanFragment> children) {
    if (children.size() != 1) {
      return false;
    }
    PlanNode childPlanNode = children.get(0).getFragmentRoot();
    return childPlanNode instanceof MailboxSendNode
        && ((MailboxSendNode) childPlanNode).getDistributionType() == RelDistribution.Type.SINGLETON;
  }

  private static boolean isLeafPlan(DispatchablePlanMetadata metadata) {
    return metadata.getScannedTables().size() == 1;
  }

  // --------------------------------------------------------------------------
  // Intermediate stage assign logic
  // --------------------------------------------------------------------------
  private void assignWorkersToIntermediateFragment(PlanFragment fragment, DispatchablePlanContext context) {
    List<PlanFragment> children = fragment.getChildren();
    Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
    DispatchablePlanMetadata metadata = metadataMap.get(fragment.getFragmentId());

    if (isPrePartitionAssignment(children, metadataMap)) {
      // If the first child is partitioned and can be inherent from this intermediate stage, use the same worker
      // assignment to avoid shuffling data.
      // When partition parallelism is configured,
      // 1. Create multiple intermediate stage workers on the same instance for each worker in the first child if the
      //    first child is a table scan. this is b/c we cannot pre-config parallelism on leaf stage thus needs fan-out.
      // 2. Ignore partition parallelism when first child is NOT table scan b/c it would've done fan-out already.
      DispatchablePlanMetadata firstChildMetadata = metadataMap.get(children.get(0).getFragmentId());
      int partitionParallelism = firstChildMetadata.getPartitionParallelism();
      Map<Integer, QueryServerInstance> childWorkerIdToServerInstanceMap =
          firstChildMetadata.getWorkerIdToServerInstanceMap();
      if (partitionParallelism == 1 || firstChildMetadata.getScannedTables().isEmpty()) {
        metadata.setWorkerIdToServerInstanceMap(childWorkerIdToServerInstanceMap);
      } else {
        int numChildWorkers = childWorkerIdToServerInstanceMap.size();
        Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = new HashMap<>();
        int workerId = 0;
        for (int i = 0; i < numChildWorkers; i++) {
          QueryServerInstance serverInstance = childWorkerIdToServerInstanceMap.get(i);
          for (int j = 0; j < partitionParallelism; j++) {
            workerIdToServerInstanceMap.put(workerId++, serverInstance);
          }
        }
        metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
      }
      metadata.setPartitionFunction(firstChildMetadata.getPartitionFunction());
    } else {
      // If the query has more than one table, it is possible that the tables could be hosted on different tenants.
      // The intermediate stage will be processed on servers randomly picked from the tenants belonging to either or
      // all of the tables in the query.
      // TODO: actually make assignment strategy decisions for intermediate stages
      List<ServerInstance> serverInstances = assignServerInstances(context);
      if (metadata.isRequiresSingletonInstance()) {
        // require singleton should return a single global worker ID with 0;
        metadata.setWorkerIdToServerInstanceMap(Collections.singletonMap(0,
            new QueryServerInstance(serverInstances.get(RANDOM.nextInt(serverInstances.size())))));
      } else {
        Map<String, String> options = context.getPlannerContext().getOptions();
        int stageParallelism = Integer.parseInt(options.getOrDefault(QueryOptionKey.STAGE_PARALLELISM, "1"));
        Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = new HashMap<>();
        int workerId = 0;
        for (ServerInstance serverInstance : serverInstances) {
          QueryServerInstance queryServerInstance = new QueryServerInstance(serverInstance);
          for (int i = 0; i < stageParallelism; i++) {
            workerIdToServerInstanceMap.put(workerId++, queryServerInstance);
          }
        }
        metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
        metadata.setPartitionFunction(DEFAULT_SHUFFLE_PARTITION_FUNCTION);
      }
    }
  }

  private boolean isPrePartitionAssignment(List<PlanFragment> children,
      Map<Integer, DispatchablePlanMetadata> metadataMap) {
    if (children.isEmpty()) {
      return false;
    }
    // Now, is all children needs to be pre-partitioned by the same function and size to allow pre-partition assignment
    // TODO:
    //   1. When partition function is allowed to be configured in exchange we can relax this condition
    //   2. Pick the most colocate assignment instead of picking the first children
    String partitionFunction = null;
    int partitionCount = 0;
    for (PlanFragment child : children) {
      DispatchablePlanMetadata childMetadata = metadataMap.get(child.getFragmentId());
      if (!childMetadata.isPrePartitioned()) {
        return false;
      }
      if (partitionFunction == null) {
        partitionFunction = childMetadata.getPartitionFunction();
      } else if (!partitionFunction.equalsIgnoreCase(childMetadata.getPartitionFunction())) {
        return false;
      }
      int childComputedPartitionCount =
          childMetadata.getWorkerIdToServerInstanceMap().size() * (isLeafPlan(childMetadata)
              ? childMetadata.getPartitionParallelism() : 1);
      if (partitionCount == 0) {
        partitionCount = childComputedPartitionCount;
      } else if (childComputedPartitionCount != partitionCount) {
        return false;
      }
    }
    return true;
  }

  private List<ServerInstance> assignServerInstances(DispatchablePlanContext context) {
    List<ServerInstance> serverInstances;
    Set<String> tableNames = context.getTableNames();
    Map<String, ServerInstance> enabledServerInstanceMap = _routingManager.getEnabledServerInstanceMap();
    if (tableNames.isEmpty()) {
      // TODO: Short circuit it when no table needs to be scanned
      // This could be the case from queries that don't actually fetch values from the tables. In such cases the
      // routing need not be tenant aware.
      // Eg: SELECT 1 AS one FROM select_having_expression_test_test_having HAVING 1 > 2;
      serverInstances = new ArrayList<>(enabledServerInstanceMap.values());
    } else {
      Set<String> servers = new HashSet<>();
      for (String tableName : tableNames) {
        TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
        if (tableType == null) {
          Set<String> offlineTableServers = _routingManager.getServingInstances(
              TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName));
          if (offlineTableServers != null) {
            servers.addAll(offlineTableServers);
          }
          Set<String> realtimeTableServers = _routingManager.getServingInstances(
              TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName));
          if (realtimeTableServers != null) {
            servers.addAll(realtimeTableServers);
          }
        } else {
          Set<String> tableServers = _routingManager.getServingInstances(tableName);
          if (tableServers != null) {
            servers.addAll(tableServers);
          }
        }
      }
      if (servers.isEmpty()) {
        // fall back to use all enabled servers if no server is found for the tables
        serverInstances = new ArrayList<>(enabledServerInstanceMap.values());
      } else {
        serverInstances = new ArrayList<>(servers.size());
        for (String server : servers) {
          ServerInstance serverInstance = enabledServerInstanceMap.get(server);
          if (serverInstance != null) {
            serverInstances.add(serverInstance);
          }
        }
      }
    }
    if (serverInstances.isEmpty()) {
      LOGGER.error("[RequestId: {}] No server instance found for intermediate stage for tables: {}",
          context.getRequestId(), tableNames);
      throw new IllegalStateException(
          "No server instance found for intermediate stage for tables: " + Arrays.toString(tableNames.toArray()));
    }
    return serverInstances;
  }

  private void assignWorkersToLeafFragment(PlanFragment fragment, DispatchablePlanContext context) {
    DispatchablePlanMetadata metadata = context.getDispatchablePlanMetadataMap().get(fragment.getFragmentId());
    Map<String, String> tableOptions = metadata.getTableOptions();
    String partitionKey =
        tableOptions != null ? tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_KEY) : null;
    if (partitionKey == null) {
      // when partition key is not given, we do not partition assign workers for leaf-stage.
      assignWorkersToNonPartitionedLeafFragment(metadata, context);
    } else {
      assignWorkersToPartitionedLeafFragment(metadata, context, partitionKey, tableOptions);
    }
  }

  // --------------------------------------------------------------------------
  // Non-partitioned leaf stage assignment
  // --------------------------------------------------------------------------
  private void assignWorkersToNonPartitionedLeafFragment(DispatchablePlanMetadata metadata,
      DispatchablePlanContext context) {
    String tableName = metadata.getScannedTables().get(0);
    Map<String, RoutingTable> routingTableMap = getRoutingTable(tableName, context.getRequestId());
    Preconditions.checkState(!routingTableMap.isEmpty(), "Unable to find routing entries for table: %s", tableName);

    // acquire time boundary info if it is a hybrid table.
    if (routingTableMap.size() > 1) {
      TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(
          TableNameBuilder.forType(TableType.OFFLINE)
              .tableNameWithType(TableNameBuilder.extractRawTableName(tableName)));
      if (timeBoundaryInfo != null) {
        metadata.setTimeBoundaryInfo(timeBoundaryInfo);
      } else {
        // remove offline table routing if no time boundary info is acquired.
        routingTableMap.remove(TableType.OFFLINE.name());
      }
    }

    // extract all the instances associated to each table type
    Map<ServerInstance, Map<String, List<String>>> serverInstanceToSegmentsMap = new HashMap<>();
    for (Map.Entry<String, RoutingTable> routingEntry : routingTableMap.entrySet()) {
      String tableType = routingEntry.getKey();
      RoutingTable routingTable = routingEntry.getValue();
      // for each server instance, attach all table types and their associated segment list.
      Map<ServerInstance, Pair<List<String>, List<String>>> segmentsMap = routingTable.getServerInstanceToSegmentsMap();
      for (Map.Entry<ServerInstance, Pair<List<String>, List<String>>> serverEntry : segmentsMap.entrySet()) {
        Map<String, List<String>> tableTypeToSegmentListMap =
            serverInstanceToSegmentsMap.computeIfAbsent(serverEntry.getKey(), k -> new HashMap<>());
        // TODO: support optional segments for multi-stage engine.
        Preconditions.checkState(tableTypeToSegmentListMap.put(tableType, serverEntry.getValue().getLeft()) == null,
            "Entry for server {} and table type: {} already exist!", serverEntry.getKey(), tableType);
      }

      // attach unavailable segments to metadata
      if (!routingTable.getUnavailableSegments().isEmpty()) {
        metadata.addUnavailableSegments(tableName, routingTable.getUnavailableSegments());
      }
    }
    int workerId = 0;
    Map<Integer, QueryServerInstance> workerIdToServerInstanceMap = new HashMap<>();
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    for (Map.Entry<ServerInstance, Map<String, List<String>>> entry : serverInstanceToSegmentsMap.entrySet()) {
      workerIdToServerInstanceMap.put(workerId, new QueryServerInstance(entry.getKey()));
      workerIdToSegmentsMap.put(workerId, entry.getValue());
      workerId++;
    }
    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
    metadata.setPartitionFunction(DEFAULT_SHUFFLE_PARTITION_FUNCTION);
  }

  /**
   * Acquire routing table for items listed in {@link TableScanNode}.
   *
   * @param tableName table name with or without type suffix.
   * @return keyed-map from table type(s) to routing table(s).
   */
  private Map<String, RoutingTable> getRoutingTable(String tableName, long requestId) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableName);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    Map<String, RoutingTable> routingTableMap = new HashMap<>();
    RoutingTable routingTable;
    if (tableType == null) {
      routingTable = getRoutingTable(rawTableName, TableType.OFFLINE, requestId);
      if (routingTable != null) {
        routingTableMap.put(TableType.OFFLINE.name(), routingTable);
      }
      routingTable = getRoutingTable(rawTableName, TableType.REALTIME, requestId);
      if (routingTable != null) {
        routingTableMap.put(TableType.REALTIME.name(), routingTable);
      }
    } else {
      routingTable = getRoutingTable(tableName, tableType, requestId);
      if (routingTable != null) {
        routingTableMap.put(tableType.name(), routingTable);
      }
    }
    return routingTableMap;
  }

  private RoutingTable getRoutingTable(String tableName, TableType tableType, long requestId) {
    String tableNameWithType =
        TableNameBuilder.forType(tableType).tableNameWithType(TableNameBuilder.extractRawTableName(tableName));
    return _routingManager.getRoutingTable(
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM \"" + tableNameWithType + "\""), requestId);
  }

  // --------------------------------------------------------------------------
  // Partitioned leaf stage assignment
  // --------------------------------------------------------------------------
  private void assignWorkersToPartitionedLeafFragment(DispatchablePlanMetadata metadata,
      DispatchablePlanContext context, String partitionKey, Map<String, String> tableOptions) {
    // when partition key exist, we assign workers for leaf-stage in partitioned fashion.

    String numPartitionsStr = tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_SIZE);
    Preconditions.checkState(numPartitionsStr != null, "'%s' must be provided for partition key: %s",
        PinotHintOptions.TableHintOptions.PARTITION_SIZE, partitionKey);
    int numPartitions = Integer.parseInt(numPartitionsStr);
    Preconditions.checkState(numPartitions > 0, "'%s' must be positive, got: %s",
        PinotHintOptions.TableHintOptions.PARTITION_SIZE, numPartitions);

    String partitionFunction = tableOptions.getOrDefault(PinotHintOptions.TableHintOptions.PARTITION_FUNCTION,
        DEFAULT_TABLE_PARTITION_FUNCTION);

    String partitionParallelismStr = tableOptions.get(PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM);
    int partitionParallelism = partitionParallelismStr != null ? Integer.parseInt(partitionParallelismStr) : 1;
    Preconditions.checkState(partitionParallelism > 0, "'%s' must be positive: %s, got: %s",
        PinotHintOptions.TableHintOptions.PARTITION_PARALLELISM, partitionParallelism);

    String tableName = metadata.getScannedTables().get(0);
    // calculates the partition table info using the routing manager
    PartitionTableInfo partitionTableInfo = calculatePartitionTableInfo(tableName);
    // verifies that the partition table obtained from routing manager is compatible with the hint options
    checkPartitionInfoMap(partitionTableInfo, tableName, partitionKey, numPartitions, partitionFunction);

    // Pick one server per partition
    // NOTE: Pick server based on the request id so that the same server is picked across different table scan when the
    //       segments for the same partition is colocated
    long indexToPick = context.getRequestId();
    PartitionInfo[] partitionInfoMap = partitionTableInfo._partitionInfoMap;
    int workerId = 0;
    Map<Integer, QueryServerInstance> workedIdToServerInstanceMap = new HashMap<>();
    Map<Integer, Map<String, List<String>>> workerIdToSegmentsMap = new HashMap<>();
    Map<String, ServerInstance> enabledServerInstanceMap = _routingManager.getEnabledServerInstanceMap();
    for (int i = 0; i < numPartitions; i++) {
      PartitionInfo partitionInfo = partitionInfoMap[i];
      // TODO: Currently we don't support the case when a partition doesn't contain any segment. The reason is that the
      //       leaf stage won't be able to directly return empty response.
      Preconditions.checkState(partitionInfo != null, "Failed to find any segment for table: %s, partition: %s",
          tableName, i);
      ServerInstance serverInstance =
          pickEnabledServer(partitionInfo._fullyReplicatedServers, enabledServerInstanceMap, indexToPick++);
      Preconditions.checkState(serverInstance != null,
          "Failed to find enabled fully replicated server for table: %s, partition: %s", tableName, i);
      workedIdToServerInstanceMap.put(workerId, new QueryServerInstance(serverInstance));
      workerIdToSegmentsMap.put(workerId, getSegmentsMap(partitionInfo));
      workerId++;
    }
    metadata.setWorkerIdToServerInstanceMap(workedIdToServerInstanceMap);
    metadata.setWorkerIdToSegmentsMap(workerIdToSegmentsMap);
    metadata.setTimeBoundaryInfo(partitionTableInfo._timeBoundaryInfo);
    metadata.setPartitionFunction(partitionFunction);
    metadata.setPartitionParallelism(partitionParallelism);
  }

  @Nullable
  public TableOptions inferTableOptions(String tableName) {
    try {
      PartitionTableInfo partitionTableInfo = calculatePartitionTableInfo(tableName);
      return ImmutableTableOptions.builder()
          .partitionFunction(partitionTableInfo._partitionFunction)
          .partitionKey(partitionTableInfo._partitionKey)
          .partitionSize(partitionTableInfo._numPartitions)
          .build();
    } catch (IllegalStateException e) {
      return null;
    }
  }

  private PartitionTableInfo calculatePartitionTableInfo(String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
      String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
      boolean offlineRoutingExists = _routingManager.routingExists(offlineTableName);
      boolean realtimeRoutingExists = _routingManager.routingExists(realtimeTableName);
      Preconditions.checkState(offlineRoutingExists || realtimeRoutingExists, "Routing doesn't exist for table: %s",
          tableName);

      if (offlineRoutingExists && realtimeRoutingExists) {
        TablePartitionInfo offlineTpi = _routingManager.getTablePartitionInfo(offlineTableName);
        Preconditions.checkState(offlineTpi != null, "Failed to find table partition info for table: %s",
            offlineTableName);
        TablePartitionInfo realtimeTpi = _routingManager.getTablePartitionInfo(realtimeTableName);
        Preconditions.checkState(realtimeTpi != null, "Failed to find table partition info for table: %s",
            realtimeTableName);
        // For hybrid table, find the common servers for each partition
        TimeBoundaryInfo timeBoundaryInfo = _routingManager.getTimeBoundaryInfo(offlineTableName);
        // Ignore OFFLINE side when time boundary info is unavailable
        if (timeBoundaryInfo == null) {
          return PartitionTableInfo.fromTablePartitionInfo(realtimeTpi, TableType.REALTIME);
        }

        verifyCompatibility(offlineTpi, realtimeTpi);

        TablePartitionInfo.PartitionInfo[] offlinePartitionInfoMap = offlineTpi.getPartitionInfoMap();
        TablePartitionInfo.PartitionInfo[] realtimePartitionInfoMap = realtimeTpi.getPartitionInfoMap();

        int numPartitions = offlineTpi.getNumPartitions();
        PartitionInfo[] partitionInfoMap = new PartitionInfo[numPartitions];
        for (int i = 0; i < numPartitions; i++) {
          TablePartitionInfo.PartitionInfo offlinePartitionInfo = offlinePartitionInfoMap[i];
          TablePartitionInfo.PartitionInfo realtimePartitionInfo = realtimePartitionInfoMap[i];
          if (offlinePartitionInfo == null && realtimePartitionInfo == null) {
            continue;
          }
          if (offlinePartitionInfo == null) {
            partitionInfoMap[i] =
                new PartitionInfo(realtimePartitionInfo._fullyReplicatedServers, null, realtimePartitionInfo._segments);
            continue;
          }
          if (realtimePartitionInfo == null) {
            partitionInfoMap[i] =
                new PartitionInfo(offlinePartitionInfo._fullyReplicatedServers, offlinePartitionInfo._segments, null);
            continue;
          }
          Set<String> fullyReplicatedServers = new HashSet<>(offlinePartitionInfo._fullyReplicatedServers);
          fullyReplicatedServers.retainAll(realtimePartitionInfo._fullyReplicatedServers);
          Preconditions.checkState(!fullyReplicatedServers.isEmpty(),
              "Failed to find fully replicated server for partition: %s in hybrid table: %s", i, tableName);
          partitionInfoMap[i] = new PartitionInfo(
              fullyReplicatedServers, offlinePartitionInfo._segments, realtimePartitionInfo._segments);
        }
        return new PartitionTableInfo(partitionInfoMap, timeBoundaryInfo, offlineTpi.getPartitionColumn(),
            numPartitions, offlineTpi.getPartitionFunctionName());
      } else if (offlineRoutingExists) {
        return getOfflinePartitionTableInfo(offlineTableName);
      } else {
        return getRealtimePartitionTableInfo(realtimeTableName);
      }
    } else {
      if (tableType == TableType.OFFLINE) {
        return getOfflinePartitionTableInfo(tableName);
      } else {
        return getRealtimePartitionTableInfo(tableName);
      }
    }
  }

  private static void verifyCompatibility(TablePartitionInfo offlineTpi, TablePartitionInfo realtimeTpi)
      throws IllegalArgumentException {
    Preconditions.checkState(offlineTpi.getPartitionColumn().equals(realtimeTpi.getPartitionColumn()),
        "Partition column mismatch for hybrid table %s: %s offline vs %s online",
        offlineTpi.getTableNameWithType(), offlineTpi.getPartitionColumn(), realtimeTpi.getPartitionColumn());
    Preconditions.checkState(offlineTpi.getNumPartitions() == realtimeTpi.getNumPartitions(),
        "Partition size mismatch for hybrid table %s: %s offline vs %s online",
        offlineTpi.getTableNameWithType(), offlineTpi.getNumPartitions(), realtimeTpi.getNumPartitions());
    Preconditions.checkState(
        offlineTpi.getPartitionFunctionName().equalsIgnoreCase(realtimeTpi.getPartitionFunctionName()),
        "Partition function mismatch for hybrid table %s: %s offline vs %s online",
        offlineTpi.getTableNameWithType(), offlineTpi.getPartitionFunctionName(),
        realtimeTpi.getPartitionFunctionName());
  }

  /**
   * Verifies that the partition info maps from the table partition info are compatible with the information supplied
   * as arguments.
   */
  private void checkPartitionInfoMap(PartitionTableInfo partitionTableInfo, String tableNameWithType,
      String partitionKey, int numPartitions, String partitionFunction) {
    Preconditions.checkState(partitionTableInfo._partitionKey.equals(partitionKey),
        "Partition key: %s does not match partition column: %s for table: %s", partitionKey,
        partitionTableInfo._partitionKey, tableNameWithType);
    Preconditions.checkState(partitionTableInfo._numPartitions == numPartitions,
        "Partition size mismatch (hint: %s, table: %s) for table: %s", numPartitions,
        partitionTableInfo._numPartitions, tableNameWithType);
    Preconditions.checkState(partitionTableInfo._partitionFunction.equalsIgnoreCase(partitionFunction),
        "Partition function mismatch (hint: %s, table: %s) for table %s", partitionFunction,
        partitionTableInfo._partitionFunction, tableNameWithType);
  }

  private PartitionTableInfo getOfflinePartitionTableInfo(String offlineTableName) {
    TablePartitionInfo offlineTpi = _routingManager.getTablePartitionInfo(offlineTableName);
    Preconditions.checkState(offlineTpi != null, "Failed to find table partition info for table: %s",
        offlineTableName);
    return PartitionTableInfo.fromTablePartitionInfo(offlineTpi, TableType.OFFLINE);
  }

  private PartitionTableInfo getRealtimePartitionTableInfo(String realtimeTableName) {
    TablePartitionInfo realtimeTpi = _routingManager.getTablePartitionInfo(realtimeTableName);
    Preconditions.checkState(realtimeTpi != null, "Failed to find table partition info for table: %s",
        realtimeTableName);
    return PartitionTableInfo.fromTablePartitionInfo(realtimeTpi, TableType.REALTIME);
  }

  private static class PartitionTableInfo {
    final PartitionInfo[] _partitionInfoMap;
    @Nullable
    final TimeBoundaryInfo _timeBoundaryInfo;
    final String _partitionKey;
    final int _numPartitions;
    final String _partitionFunction;

    PartitionTableInfo(PartitionInfo[] partitionInfoMap, @Nullable TimeBoundaryInfo timeBoundaryInfo,
        String partitionKey, int numPartitions, String partitionFunction) {
      _partitionInfoMap = partitionInfoMap;
      _timeBoundaryInfo = timeBoundaryInfo;
      _partitionKey = partitionKey;
      _numPartitions = numPartitions;
      _partitionFunction = partitionFunction;
    }

    public static PartitionTableInfo fromTablePartitionInfo(
        TablePartitionInfo tablePartitionInfo, TableType tableType) {
      if (!tablePartitionInfo.getSegmentsWithInvalidPartition().isEmpty()) {
        throw new IllegalStateException("Find " + tablePartitionInfo.getSegmentsWithInvalidPartition().size()
            + " segments with invalid partition");
      }

      int numPartitions = tablePartitionInfo.getNumPartitions();
      TablePartitionInfo.PartitionInfo[] tablePartitionInfoMap = tablePartitionInfo.getPartitionInfoMap();
      PartitionInfo[] workerPartitionInfoMap = new PartitionInfo[numPartitions];
      for (int i = 0; i < numPartitions; i++) {
        TablePartitionInfo.PartitionInfo partitionInfo = tablePartitionInfoMap[i];
        if (partitionInfo != null) {
          switch (tableType) {
            case OFFLINE:
              workerPartitionInfoMap[i] =
                  new PartitionInfo(partitionInfo._fullyReplicatedServers, partitionInfo._segments, null);
              break;
            case REALTIME:
              workerPartitionInfoMap[i] =
                  new PartitionInfo(partitionInfo._fullyReplicatedServers, null, partitionInfo._segments);
              break;
            default:
              throw new IllegalStateException("Unsupported table type: " + tableType);
          }
        }
      }
      return new PartitionTableInfo(workerPartitionInfoMap, null, tablePartitionInfo.getPartitionColumn(),
          numPartitions, tablePartitionInfo.getPartitionFunctionName());
    }
  }

  private static class PartitionInfo {
    final Set<String> _fullyReplicatedServers;
    final List<String> _offlineSegments;
    final List<String> _realtimeSegments;

    PartitionInfo(Set<String> fullyReplicatedServers, @Nullable List<String> offlineSegments,
        @Nullable List<String> realtimeSegments) {
      _fullyReplicatedServers = fullyReplicatedServers;
      _offlineSegments = offlineSegments;
      _realtimeSegments = realtimeSegments;
    }
  }

  /**
   * Picks an enabled server deterministically based on the given index to pick.
   */
  @Nullable
  private static ServerInstance pickEnabledServer(Set<String> candidates,
      Map<String, ServerInstance> enabledServerInstanceMap, long indexToPick) {
    int numCandidates = candidates.size();
    if (numCandidates == 0) {
      return null;
    }
    if (numCandidates == 1) {
      return enabledServerInstanceMap.get(candidates.iterator().next());
    }
    List<String> candidateList = new ArrayList<>(candidates);
    candidateList.sort(null);
    int startIndex = (int) ((indexToPick & Long.MAX_VALUE) % numCandidates);
    for (int i = 0; i < numCandidates; i++) {
      String server = candidateList.get((startIndex + i) % numCandidates);
      ServerInstance serverInstance = enabledServerInstanceMap.get(server);
      if (serverInstance != null) {
        return serverInstance;
      }
    }
    return null;
  }

  private static Map<String, List<String>> getSegmentsMap(PartitionInfo partitionInfo) {
    Map<String, List<String>> segmentsMap = new HashMap<>();
    if (partitionInfo._offlineSegments != null) {
      segmentsMap.put(TableType.OFFLINE.name(), partitionInfo._offlineSegments);
    }
    if (partitionInfo._realtimeSegments != null) {
      segmentsMap.put(TableType.REALTIME.name(), partitionInfo._realtimeSegments);
    }
    return segmentsMap;
  }
}
