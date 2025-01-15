package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.DataMseBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;


public class HashJoinReactiveOperator extends ReactiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(HashJoinOperator.class);
  private static final String EXPLAIN_NAME = "HASH_JOIN";
  private static final int INITIAL_HEURISTIC_SIZE = 16;
  private static final int DEFAULT_MAX_ROWS_IN_JOIN = 1024 * 1024; // 2^20, around 1MM rows
  private static final CommonConstants.MultiStageQueryRunner.JoinOverFlowMode DEFAULT_JOIN_OVERFLOW_MODE =
      CommonConstants.MultiStageQueryRunner.JoinOverFlowMode.THROW;

  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES =
      Set.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT, JoinRelType.FULL, JoinRelType.SEMI,
          JoinRelType.ANTI);

  private final ReactiveOperator _leftInput;
  private final ReactiveOperator _rightInput;
  private final JoinRelType _joinType;
  private final KeySelector<?> _leftKeySelector;
  private final KeySelector<?> _rightKeySelector;
  private final DataSchema _resultSchema;
  private final int _leftColumnSize;
  private final int _resultColumnSize;
  private final List<TransformOperand> _nonEquiEvaluators;
  private final StatMap<HashJoinOperator.StatKey> _statMap = new StatMap<>(HashJoinOperator.StatKey.class);

  // Below are specific parameters to protect the server from a very large join operation.
  // Once the hash table reaches the limit, we will throw exception or break the right table build process.
  // The limit also applies to the number of joined rows in blocks from the left table.
  /**
   * Max rows allowed to build the right table hash collection. Also max rows emitted in each join with a block from
   * the left table.
   */
  private final int _maxRowsInJoin;
  /**
   * Mode when join overflow happens, supported values: THROW or BREAK.
   *   THROW(default): Break right table build process, and throw exception, no JOIN with left table performed.
   *   BREAK: Break right table build process, continue to perform JOIN operation, results might be partial.
   */
  private final CommonConstants.MultiStageQueryRunner.JoinOverFlowMode _joinOverflowMode;

  public HashJoinReactiveOperator(OpChainExecutionContext context,
      ReactiveOperator leftInput,
      ReactiveOperator rightInput,
      JoinNode node,
      DataSchema resultSchema,
      int leftColumnSize,
      int resultColumnSize) {
    super(context);
    _leftInput = leftInput;
    _rightInput = rightInput;
    _joinType = node.getJoinType();
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(_joinType), "Join type: % is not supported for hash join",
        _joinType);
    _leftKeySelector = KeySelectorFactory.getKeySelector(node.getLeftKeys());
    _rightKeySelector = KeySelectorFactory.getKeySelector(node.getRightKeys());
    _resultSchema = resultSchema;
    _leftColumnSize = leftColumnSize;
    _resultColumnSize = resultColumnSize;
    List<RexExpression> nonEquiConditions = node.getNonEquiConditions();
    _nonEquiEvaluators = new ArrayList<>(nonEquiConditions.size());
    for (RexExpression nonEquiCondition : nonEquiConditions) {
      _nonEquiEvaluators.add(TransformOperandFactory.getTransformOperand(nonEquiCondition, _resultSchema));
    }
    Map<String, String> metadata = context.getOpChainMetadata();
    PlanNode.NodeHint nodeHint = node.getNodeHint();
    _maxRowsInJoin = HashJoinOperator.getMaxRowsInJoin(metadata, nodeHint);
    _joinOverflowMode = HashJoinOperator.getJoinOverflowMode(metadata, nodeHint);
  }

  // Samples resource usage of the operator. The operator should call this function for every block of data or
  // assuming the block holds 10000 rows or more.
  protected boolean sampleAndCheckInterruption() {
    Tracing.ThreadAccountantOps.sampleMSE();
    return Tracing.ThreadAccountantOps.isInterrupted();
  }

  @Override
  public Flux<DataMseBlock> toFlux() {
    Mono<RightState> rightStateMono = consumeRightChild();
    Flux<DataMseBlock> leftJoin = rightStateMono
        .flatMapMany(rightState -> _leftInput.toFlux().map(rightState::join))
        .doFinally(signalType -> {
          calculateStats()
        });
    if (_joinType != JoinRelType.RIGHT && _joinType != JoinRelType.FULL) {
      return leftJoin;
    }
    return leftJoin.concatWith(rightStateMono.map(RightState::unmatchedRightRows));
  }

  private Mono<RightState> consumeRightChild() {
    RightState rightState = new RightState();
    AtomicLong start = new AtomicLong();

    return _rightInput.toFlux()
        .doFirst(() -> {
          start.set(System.currentTimeMillis());
        })
        .handle((DataMseBlock block, SynchronousSink<RightState> sink) -> {
          List<Object[]> container = block.getRows();
          // Row based overflow check.
          if (container.size() + rightState._numRowsInHashTable > _maxRowsInJoin) {
            if (_joinOverflowMode == CommonConstants.MultiStageQueryRunner.JoinOverFlowMode.THROW) {
              sink.error(createProcessingExceptionForJoinRowLimitExceeded(
                  "Cannot build in memory hash table for join operator, reached number of rows limit: "
                      + _maxRowsInJoin));
            } else {
              // Just fill up the buffer.
              int remainingRows = _maxRowsInJoin - rightState._numRowsInHashTable;
              container = container.subList(0, remainingRows);
              _statMap.merge(HashJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
              // setting only the rightTableOperator to be early terminated
              sink.complete();
            }
          }
          // put all the rows into corresponding hash collections keyed by the key selector function.
          for (Object[] row : container) {
            ArrayList<Object[]> hashCollection = rightState.getRowsFor(row);
            int size = hashCollection.size();
            if ((size & size - 1) == 0 && size < _maxRowsInJoin && size < Integer.MAX_VALUE / 2) { // is power of 2
              hashCollection.ensureCapacity(Math.min(size << 1, _maxRowsInJoin));
            }
            hashCollection.add(row);
          }
          rightState._numRowsInHashTable += container.size();
          if (sampleAndCheckInterruption()) {
            sink.error(new ProcessingException(QueryException.QUERY_CANCELLATION_ERROR)); // TODO verify errorCode
          } else {
            sink.next(rightState);
          }
        })
        .doFinally(signalType -> {
          long duration = System.currentTimeMillis() - start.get();
          _statMap.merge(HashJoinOperator.StatKey.TIME_BUILDING_HASH_TABLE_MS, duration);
        })
        .last();
  }

  private ProcessingException createProcessingExceptionForJoinRowLimitExceeded(String reason) {
    ProcessingException resourceLimitExceededException =
        new ProcessingException(QueryException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE);
    resourceLimitExceededException.setMessage(reason
        + ". Consider increasing the limit for the maximum number of rows in a join either via the query option '"
        + CommonConstants.Broker.Request.QueryOptionKey.MAX_ROWS_IN_JOIN + "' or the '"
        + PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN + "' hint in the '" + PinotHintOptions.JOIN_HINT_OPTIONS
        + "'. Alternatively, if partial results are acceptable, the join overflow mode can be set to '"
        + CommonConstants.MultiStageQueryRunner.JoinOverFlowMode.BREAK.name() + "' either via the query option '"
        + CommonConstants.Broker.Request.QueryOptionKey.JOIN_OVERFLOW_MODE + "' or the '"
        + PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE + "' hint in the '" + PinotHintOptions.JOIN_HINT_OPTIONS
        + "'.");
    return resourceLimitExceededException;
  }

  @Override
  public MultiStageQueryStats calculateStats() {
    MultiStageQueryStats rightStats = _rightInput.calculateStats();
    MultiStageQueryStats leftStats = _leftInput.calculateStats();
    leftStats.mergeInOrder(rightStats, MultiStageOperator.Type.HASH_JOIN, _statMap)
    return leftStats;
  }

  @Override
  public List<ReactiveOperator> getChildOperators() {
    return List.of();
  }

  class RightState {
    Map<Object, ArrayList<Object[]>> _broadcastRightTable = new HashMap<>();
    int _numRowsInHashTable = 0;

    // Used to track matched right rows.
    // Only used for right join and full join to output non-matched right rows.
    private final Map<Object, BitSet> _matchedRightRows = new HashMap<>();

    public DataMseBlock join(DataMseBlock left) {
      // like HashJoinOperator#buildJoinedRows(TransferableBlock leftBlock)
    }

    public ArrayList<Object[]> getRowsFor(Object[] row) {
      Object key = _rightKeySelector.getKey(row);
      return _broadcastRightTable.computeIfAbsent(key, k -> new ArrayList<>(INITIAL_HEURISTIC_SIZE));
    }

    public DataMseBlock unmatchedRightRows() {
      // like HashJoinOperator#buildNonMatchRightRows
    }
  }
}
