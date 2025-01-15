package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.function.Function;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.runtime.blocks.DataMseBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.exchange.BlockExchange;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;


public class MailboxSendReactiveOperator extends ReactiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxSendReactiveOperator.class);

  private final ReactiveOperator _input;
  private final BlockExchange _exchange;
  private final StatMap<MailboxSendOperator.StatKey> _statMap = new StatMap<>(MailboxSendOperator.StatKey.class);

  public MailboxSendReactiveOperator(OpChainExecutionContext context, ReactiveOperator input, MailboxSendNode node) {
    this(context, input,
        statMap -> MailboxSendOperator.getBlockExchange(context, node.getReceiverStageId(), node.getDistributionType(),
            node.getKeys(), statMap));
    _statMap.merge(MailboxSendOperator.StatKey.STAGE, context.getStageId());
    _statMap.merge(MailboxSendOperator.StatKey.PARALLELISM, 1);
  }

  public MailboxSendReactiveOperator(OpChainExecutionContext context,
      ReactiveOperator input, Function<StatMap<MailboxSendOperator.StatKey>, BlockExchange> exchangeFactory) {
    super(context);
    _input = input;
    _exchange = exchangeFactory.apply(_statMap);
  }

  @Override
  public Flux<DataMseBlock> toFlux() {
    return _input.toFlux()
        .subscribeOn(_context.getNormalScheduler())
        .handle((DataMseBlock block, SynchronousSink<DataMseBlock> sink) -> {
          TransferableBlock transferableBlock = getTransferableBlock(block);
          try {
            boolean isEarlyTerminated = sendTransferableBlock(transferableBlock);
            if (isEarlyTerminated) {
              sink.complete();
            } else {
              sink.next(block);
            }
          } catch (Exception e) {
            sink.error(e);
          }
        })
        .doFinally(signalType -> {
          switch (signalType) {
            case CANCEL:
            case ON_COMPLETE:
              try {
                MultiStageQueryStats multiStageQueryStats = calculateStats();
                TransferableBlock eosBlock = TransferableBlockUtils.getEndOfStreamTransferableBlock(
                    multiStageQueryStats);
                sendTransferableBlock(eosBlock);
                // After sending its own stats, the sending operator of the stage 1 has the complete view of all stats
                // Therefore this is the only place we can update some of the metrics like total seen rows or time spent.
                if (_context.getStageId() == 1) {
                  MailboxSendOperator.updateMetrics(eosBlock);
                }
              } catch (Exception e) {
                LOGGER.error("Caught exception while finishing the exchange", e);
              }
            default:
              LOGGER.info("No stats send from {} given it finished with signal {}", _context.getId(), signalType);
          }
        })
        .doOnError(throwable -> {
          if (!(throwable instanceof Exception)) {
            LOGGER.error("Caught exception while sending the block", throwable);
            throw (Error) throwable;
          } else {
            TransferableBlock errorBlock = TransferableBlockUtils.getErrorTransferableBlock((Exception) throwable);
            try {
              sendTransferableBlock(errorBlock);
            } catch (Exception e) {
              LOGGER.error("Caught exception while sending the error block", e);
            }
          }
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

  private TransferableBlock getTransferableBlock(DataMseBlock block) {
    // TODO: Implement this method
  }

  private boolean sendTransferableBlock(TransferableBlock block)
      throws Exception {
    boolean isEarlyTerminated = _exchange.send(block);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("==[SEND]== Block " + block + " sent from: " + _context.getId());
    }
    return isEarlyTerminated;
  }
}
