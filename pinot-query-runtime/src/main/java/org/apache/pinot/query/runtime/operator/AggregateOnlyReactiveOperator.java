package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.query.runtime.blocks.DataMseBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import reactor.core.publisher.Flux;


public class AggregateOnlyReactiveOperator extends ReactiveOperator {
  private final ReactiveOperator _input;
  private static final CountAggregationFunction COUNT_STAR_AGG_FUNCTION =
      new CountAggregationFunction(Collections.singletonList(ExpressionContext.forIdentifier("*")), false);

  private final DataSchema _resultSchema;
  private final MultistageAggregationExecutor _aggregationExecutor;
  @Nullable
  private TransferableBlock _eosBlock;
  private final StatMap<AggregateOperator.StatKey> _statMap = new StatMap<>(AggregateOperator.StatKey.class);

  public AggregateOnlyReactiveOperator(OpChainExecutionContext context, ReactiveOperator input,
      DataSchema resultSchema,
      MultistageAggregationExecutor aggregationExecutor) {
    super(context);
    _input = input;
    _resultSchema = resultSchema;
    _aggregationExecutor = aggregationExecutor;
  }

  @Override
  public Flux<DataMseBlock> toFlux() {
    return _input.toFlux()
        .reduce(_aggregationExecutor, (state, block) -> {
          state.processBlock(block);
          return state;
        })
        .map(state -> DataMseBlock.fromRows(state.getResult(), _resultSchema))
        .flux();
  }

  @Override
  public MultiStageQueryStats calculateStats() {
    MultiStageQueryStats holder = _input.calculateStats();
    Preconditions.checkArgument(holder.getCurrentStageId() == _context.getStageId(),
        "The holder's stage id should be the same as the current operator's stage id. Expected %s, got %s",
        _context.getStageId(), holder.getCurrentStageId());
    holder.getCurrentStats().addLastOperator(MultiStageOperator.Type.FILTER, _statMap);
    return holder;
  }

  @Override
  public List<ReactiveOperator> getChildOperators() {
    return List.of();
  }
}
