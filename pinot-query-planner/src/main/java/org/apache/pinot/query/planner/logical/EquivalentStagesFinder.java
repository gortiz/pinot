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
package org.apache.pinot.query.planner.logical;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>EquivalentStagesFinder</code> class finds equivalent stages in the query plan.
 *
 * Equivalent stages are stages that represent the same job to be done. These stages can then be optimized to execute
 * that job only once and then broadcast the results to all the equivalent stages.
 */
public class EquivalentStagesFinder {
  public static final Logger LOGGER = LoggerFactory.getLogger(EquivalentStagesFinder.class);

  private EquivalentStagesFinder() {
  }

  public static Set<EquivalentStages> findEquivalentStages(PlanNode planNode) {
    Visitor visitor = new Visitor();
    planNode.visit(visitor, null);
    return visitor._equivalentStages;
  }

  public static class EquivalentStages {
    private final MailboxSendNode _example;
    private final Set<MailboxSendNode> _equivalentStages = Collections.newSetFromMap(new IdentityHashMap<>());

    public EquivalentStages(MailboxSendNode stage1, MailboxSendNode stage2) {
      _example = stage1;
      _equivalentStages.add(stage1);
      _equivalentStages.add(stage2);
    }

    public MailboxSendNode getExample() {
      return _example;
    }

    public void addEquivalentStage(MailboxSendNode stage) {
      _equivalentStages.add(stage);
    }
  }

  private static class Visitor extends PlanNodeVisitor.DepthFirstVisitor<Void, Void> {
    private final Set<MailboxSendNode> _uniqueVisitedRoots = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<EquivalentStages> _equivalentStages = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Map<MailboxSendNode, EquivalentStages> _equivaleceMap = new IdentityHashMap<>();
    private final NodeEquivalence _nodeEquivalence = new NodeEquivalence();

    @Override
    protected Void defaultCase(PlanNode node, Void context) {
      return null;
    }

    @Override
    public Void visitMailboxSend(MailboxSendNode node, Void context) {
      visitChildren(node, context);

      for (EquivalentStages equivalentStages : _equivalentStages) {
        if (_nodeEquivalence.areEquivalent(node, equivalentStages.getExample())) {
          equivalentStages.addEquivalentStage(node);
          _equivaleceMap.put(node, equivalentStages);
          return null;
        }
      }
      for (MailboxSendNode visitedRoot : _uniqueVisitedRoots) {
        if (_nodeEquivalence.areEquivalent(node, visitedRoot)) {
          EquivalentStages equivalentStages = new EquivalentStages(visitedRoot, node);
          _equivaleceMap.put(visitedRoot, equivalentStages);
          _equivaleceMap.put(node, equivalentStages);
          return null;
        }
      }
      _uniqueVisitedRoots.add(node);
      return null;
    }

    private class NodeEquivalence implements PlanNodeVisitor<Boolean, PlanNode> {

      public boolean areEquivalent(PlanNode node1, PlanNode node2) {
        return node1.visit(this, node2);
      }

      private boolean baseNode(PlanNode node1, PlanNode node2) {
        //@formatter:off
        // TODO: DataSchema equality checks enforce order between columns. This is probably not needed for equivalence
        //  checks, but may require some permutations. We are not changing this for now.
        boolean tempEquivalence = Objects.equals(node1.getDataSchema(), node2.getDataSchema())
            && Objects.equals(node1.getNodeHint(), node2.getNodeHint());
        //@formatter:on
        if (!tempEquivalence) {
          return false;
        }
        List<PlanNode> inputs1 = node1.getInputs();
        List<PlanNode> inputs2 = node2.getInputs();
        if (inputs1.size() != inputs2.size()) {
          return false;
        }
        for (int i = 0; i < inputs1.size(); i++) {
          if (!areEquivalent(inputs1.get(i), inputs2.get(i))) {
            return false;
          }
        }
        return true;
      }

      @Override
      public Boolean visitMailboxReceive(MailboxReceiveNode node1, PlanNode node2) {
        if (!(node2 instanceof MailboxReceiveNode)) {
          return false;
        }
        MailboxReceiveNode that = (MailboxReceiveNode) node2;
        MailboxSendNode node1Sender = node1.getSender();
        Preconditions.checkNotNull(node1Sender);
        MailboxSendNode node2Sender = that.getSender();
        Preconditions.checkNotNull(node2Sender);

        // we want to check equivalence on nodes, not equality
        if (!areEquivalent(node1Sender, node2Sender)) {
          return false;
        }

        //@formatter:off
        return baseNode(node1, node2)
            // Commented out fields are used in equals() method of MailboxReceiveNode but not needed for equivalence.
            // sender stage id will be different for sure, but we want (and already did) to compare sender equivalence
            // instead
//          && node1.getSenderStageId() == that.getSenderStageId()

            // TODO: Keys should probably be removed from the equivalence check, but would require to verify both
            //  keys are present in the data schema. We are not doing that for now.
            && Objects.equals(node1.getKeys(), that.getKeys())
            // Distribution type is not needed for equivalence. We deal with difference distribution types in the
            // spooling logic.
//          && node1.getDistributionType() == that.getDistributionType()
            // TODO: Sort, sort on sender and collations can probably be removed from the equivalence check, but would
            //  require some extra checks or transformation on the spooling logic. We are not doing that for now.
            && node1.isSort() == that.isSort()
            && node1.isSortedOnSender() == that.isSortedOnSender()
            && Objects.equals(node1.getCollations(), that.getCollations())
            && node1.getExchangeType() == that.getExchangeType();
        //@formatter:on
      }

      @Override
      public Boolean visitMailboxSend(MailboxSendNode node1, PlanNode node2) {
        if (!(node2 instanceof MailboxSendNode)) {
          return false;
        }
        MailboxSendNode that = (MailboxSendNode) node2;
        if (_equivaleceMap.containsKey(that)) {
          return areEquivalent(node1, _equivaleceMap.get(that).getExample());
        }
        //@formatter:off
        return baseNode(node1, node2)
            // Commented out fields are used in equals() method of MailboxSendNode but not needed for equivalence.
            // Receiver stage is not important for equivalence
//            && node1.getReceiverStageId() == that.getReceiverStageId()
            && node1.getExchangeType() == that.getExchangeType()
            // Distribution type is not needed for equivalence. We deal with difference distribution types in the
            // spooling logic.
//            && Objects.equals(node1.getDistributionType(), that.getDistributionType())
            // TODO: Keys should probably be removed from the equivalence check, but would require to verify both
            //  keys are present in the data schema. We are not doing that for now.
            && Objects.equals(node1.getKeys(), that.getKeys())
            // TODO: Pre-partitioned and collations can probably be removed from the equivalence check, but would
            //  require some extra checks or transformation on the spooling logic. We are not doing that for now.
            && node1.isPrePartitioned() == that.isPrePartitioned()
            && Objects.equals(node1.getCollations(), that.getCollations());
        //@formatter:on
      }

      @Override
      public Boolean visitAggregate(AggregateNode node1, PlanNode node2) {
        if (!(node2 instanceof AggregateNode)) {
          return false;
        }
        AggregateNode that = (AggregateNode) node2;
        //@formatter:off
        return baseNode(node1, node2) && Objects.equals(node1.getAggCalls(), that.getAggCalls())
            && Objects.equals(node1.getFilterArgs(), that.getFilterArgs())
            && Objects.equals(node1.getGroupKeys(), that.getGroupKeys())
            && node1.getAggType() == that.getAggType();
        //@formatter:on
      }

      @Override
      public Boolean visitFilter(FilterNode node1, PlanNode node2) {
        if (!(node2 instanceof FilterNode)) {
          return false;
        }
        FilterNode that = (FilterNode) node2;
        return baseNode(node1, node2) && Objects.equals(node1.getCondition(), that.getCondition());
      }

      @Override
      public Boolean visitJoin(JoinNode node1, PlanNode node2) {
        if (!(node2 instanceof JoinNode)) {
          return false;
        }
        JoinNode that = (JoinNode) node2;
        //@formatter:off
        return baseNode(node1, node2) && Objects.equals(node1.getJoinType(), that.getJoinType())
            && Objects.equals(node1.getLeftKeys(), that.getLeftKeys())
            && Objects.equals(node1.getRightKeys(), that.getRightKeys())
            && Objects.equals(node1.getNonEquiConditions(), that.getNonEquiConditions());
        //@formatter:on
      }

      @Override
      public Boolean visitProject(ProjectNode node1, PlanNode node2) {
        if (!(node2 instanceof ProjectNode)) {
          return false;
        }
        ProjectNode that = (ProjectNode) node2;
        return baseNode(node1, node2) && Objects.equals(node1.getProjects(), that.getProjects());
      }

      @Override
      public Boolean visitSort(SortNode node1, PlanNode node2) {
        if (!(node2 instanceof SortNode)) {
          return false;
        }
        SortNode that = (SortNode) node2;
        //@formatter:off
        return baseNode(node1, node2)
            && node1.getFetch() == that.getFetch()
            && node1.getOffset() == that.getOffset()
            && Objects.equals(node1.getCollations(), that.getCollations());
        //@formatter:on
      }

      @Override
      public Boolean visitTableScan(TableScanNode node1, PlanNode node2) {
        if (!(node2 instanceof TableScanNode)) {
          return false;
        }
        TableScanNode that = (TableScanNode) node2;
        //@formatter:off
        return baseNode(node1, node2)
            && Objects.equals(node1.getTableName(), that.getTableName())
            && Objects.equals(node1.getColumns(), that.getColumns());
        //@formatter:on
      }

      @Override
      public Boolean visitValue(ValueNode node1, PlanNode node2) {
        if (!(node2 instanceof ValueNode)) {
          return false;
        }
        ValueNode that = (ValueNode) node2;
        return baseNode(node1, node2) && Objects.equals(node1.getLiteralRows(), that.getLiteralRows());
      }

      @Override
      public Boolean visitWindow(WindowNode node1, PlanNode node2) {
        if (!(node2 instanceof WindowNode)) {
          return false;
        }
        WindowNode that = (WindowNode) node2;
        //@formatter:off
        return baseNode(node1, node2)
            && node1.getLowerBound() == that.getLowerBound()
            && node1.getUpperBound() == that.getUpperBound()
            && Objects.equals(node1.getAggCalls(), that.getAggCalls())
            && Objects.equals(node1.getKeys(), that.getKeys())
            && Objects.equals(node1.getCollations(), that.getCollations())
            && node1.getWindowFrameType() == that.getWindowFrameType()
            && Objects.equals(node1.getConstants(), that.getConstants());
        //@formatter:on
      }

      @Override
      public Boolean visitSetOp(SetOpNode node1, PlanNode node2) {
        if (!(node2 instanceof SetOpNode)) {
          return false;
        }
        SetOpNode that = (SetOpNode) node2;
        //@formatter:off
        return baseNode(node1, node2)
            && node1.getSetOpType() == that.getSetOpType()
            && node1.isAll() == that.isAll();
        //@formatter:on
      }

      @Override
      public Boolean visitExchange(ExchangeNode node1, PlanNode node2) {
        throw new UnsupportedOperationException("ExchangeNode should not be visited by NodeEquivalence");
      }

      @Override
      public Boolean visitExplained(ExplainedNode node1, PlanNode node2) {
        throw new UnsupportedOperationException("ExplainedNode should not be visited by NodeEquivalence");
      }
    }
  }
}
