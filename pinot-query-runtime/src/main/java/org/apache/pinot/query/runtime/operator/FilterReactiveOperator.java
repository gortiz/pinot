package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.DataMseBlock;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.BooleanUtils;
import reactor.core.publisher.Flux;


public class FilterReactiveOperator extends ReactiveOperator {
  private final ReactiveOperator _input;
  private final TransformOperand _filterOperand;
  private final DataSchema _dataSchema;
  private final StatMap<FilterOperator.StatKey> _statMap = new StatMap<>(FilterOperator.StatKey.class);

  public FilterReactiveOperator(ReactiveOperator input, OpChainExecutionContext context,
      TransformOperand filterOperand,
      DataSchema dataSchema) {
    super(context);
    _input = input;
    _filterOperand = filterOperand;
    _dataSchema = dataSchema;
  }

  @Override
  public Flux<DataMseBlock> toFlux() {
    return _input.toFlux()
        .flatMap(block -> {
          long startTime = System.currentTimeMillis();
          List<Object[]> rows = new ArrayList<>();
          for (Object[] row : block.getRows()) {
            Object filterResult = _filterOperand.apply(row);
            if (BooleanUtils.isTrueInternalValue(filterResult)) {
              rows.add(row);
            }
          }
          Flux<DataMseBlock> result;
          int size = rows.size();
          if (size != 0) {
            result = Flux.just(DataMseBlock.fromRows(rows, block.getDataSchema()));
            _statMap.merge(FilterOperator.StatKey.EMITTED_ROWS, size);
          } else {
            result = Flux.empty();
          }
          _statMap.merge(FilterOperator.StatKey.EXECUTION_TIME_MS, System.currentTimeMillis() - startTime);
          return result;
        });
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
    return List.of(_input);
  }
}
