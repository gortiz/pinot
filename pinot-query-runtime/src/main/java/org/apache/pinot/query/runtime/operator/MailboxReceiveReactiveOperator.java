package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.runtime.blocks.DataMseBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;


public class MailboxReceiveReactiveOperator extends ReactiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveReactiveOperator.class);

  protected final MailboxService _mailboxService;
  protected final RelDistribution.Type _distributionType;
  protected final List<String> _mailboxIds;
  protected final List<StatMap<ReceivingMailbox.StatKey>> _receivingStats;
  protected final StatMap<BaseMailboxReceiveOperator.StatKey> _statMap = new StatMap<>(
      BaseMailboxReceiveOperator.StatKey.class);

  public MailboxReceiveReactiveOperator(OpChainExecutionContext context, MailboxReceiveNode node) {
    super(context);
    _mailboxService = context.getMailboxService();
    RelDistribution.Type distributionType = node.getDistributionType();
    Preconditions.checkState(MailboxSendOperator.SUPPORTED_EXCHANGE_TYPES.contains(distributionType),
        "Unsupported exchange type: %s", distributionType);
    _distributionType = distributionType;

    long requestId = context.getRequestId();
    int senderStageId = node.getSenderStageId();
    MailboxInfos mailboxInfos = context.getWorkerMetadata().getMailboxInfosMap().get(senderStageId);
    if (mailboxInfos != null) {
      _mailboxIds =
          MailboxIdUtils.toMailboxIds(requestId, senderStageId, mailboxInfos.getMailboxInfos(), context.getStageId(),
              context.getWorkerId());
      int numMailboxes = _mailboxIds.size();
      List<BaseMailboxReceiveOperator.ReadMailboxAsyncStream> asyncStreams = new ArrayList<>(numMailboxes);
      _receivingStats = new ArrayList<>(numMailboxes);
      for (String mailboxId : _mailboxIds) {
        Flux<TransferableBlock> asFlux = _mailboxService.getAsFlux(mailboxId);
        BaseMailboxReceiveOperator.ReadMailboxAsyncStream asyncStream =
            new BaseMailboxReceiveOperator.ReadMailboxAsyncStream(asFlux, this);
        asyncStreams.add(asyncStream);
        _receivingStats.add(asyncStream._mailbox.getStatMap());
      }
    } else {
      // TODO: Revisit if we should throw exception here.
      _mailboxIds = List.of();
      _receivingStats = List.of();
    }
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.FAN_IN, _mailboxIds.size());
  }

  @Override
  public Flux<DataMseBlock> toFlux() {
    return Flux.merge(1, Flux.fromIterable(_mailboxIds).flatMap(_mailboxService::getAsFlux))
        .handle((TransferableBlock block, SynchronousSink<TransferableBlock> sink) -> {
          if (block.isDataBlock()) {
            updateState(block);
            sink.next(block);
          } else if (block.isEndOfStreamBlock()) {
            updateState(block);
            sink.complete();
          } else {
            sink.error(block.getExceptions());
          }
        })
        .map(TransferableBlock::getDataMseBlock);
  }

  @Override
  public MultiStageQueryStats calculateStats() {
    MultiStageQueryStats holder = ...;
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
