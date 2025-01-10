package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Joiner;
import java.util.List;
import org.apache.pinot.query.runtime.blocks.DataMseBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import reactor.core.publisher.Flux;


public abstract class ReactiveOperator {
  protected final OpChainExecutionContext _context;
  protected final String _operatorId;

  public ReactiveOperator(OpChainExecutionContext context) {
    _context = context;
    _operatorId = Joiner.on("_").join(getClass().getSimpleName(), _context.getStageId(), _context.getServer());
  }

  public abstract Flux<DataMseBlock> toFlux();

  public abstract MultiStageQueryStats calculateStats();

  public abstract List<ReactiveOperator> getChildOperators();
}
