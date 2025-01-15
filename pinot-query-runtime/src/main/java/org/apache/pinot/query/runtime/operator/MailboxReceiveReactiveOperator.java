package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.runtime.blocks.DataMseBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.BlockingMultiStreamConsumer;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Scheduler;


public class MailboxReceiveReactiveOperator extends ReactiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveReactiveOperator.class);

  protected final MailboxService _mailboxService;
  protected final RelDistribution.Type _distributionType;
  protected final List<String> _mailboxIds;
  protected final BlockingMultiStreamConsumer.OfTransferableBlock _multiConsumer;
  protected final List<StatMap<ReceivingMailbox.StatKey>> _receivingStats;
  protected final StatMap<BaseMailboxReceiveOperator.StatKey> _statMap = new StatMap<>(
      BaseMailboxReceiveOperator.StatKey.class);
  private MultiStageQueryStats _holder;

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
        BaseMailboxReceiveOperator.ReadMailboxAsyncStream asyncStream =
            new BaseMailboxReceiveOperator.ReadMailboxAsyncStream(
                _mailboxService.getReceivingMailbox(mailboxId), _mailboxService::releaseReceivingMailbox);
        asyncStreams.add(asyncStream);
        _receivingStats.add(asyncStream._mailbox.getStatMap());
      }
      _multiConsumer = new BlockingMultiStreamConsumer.OfTransferableBlock(context, asyncStreams);
    } else {
      // TODO: Revisit if we should throw exception here.
      _mailboxIds = List.of();
      _receivingStats = List.of();
      _multiConsumer = new BlockingMultiStreamConsumer.OfTransferableBlock(context, List.of());
    }
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.FAN_IN, _mailboxIds.size());
  }

  @Override
  public Flux<DataMseBlock> toFlux() {
    Supplier<BlockingMultiStreamConsumer.OfTransferableBlock> stateSupplier = () -> _multiConsumer;
    Consumer<BlockingMultiStreamConsumer.OfTransferableBlock> stateConsumer
        = BlockingMultiStreamConsumer::earlyTerminate;
    return Flux.generate((SynchronousSink<DataMseBlock> sink) -> {
      TransferableBlock block = _multiConsumer.readBlockBlocking();
      if (block.isDataBlock()) {
        sink.next(DataMseBlock.fromTransferableBlock(block));
      } else if (block.isErrorBlock()) {
        sink.error(extractException(block.getExceptions()));
      } else if (block.isSuccessfulEndOfStreamBlock()) {
        updateStats(block);
        sink.complete();
      }
    })
        .doOnCancel(() -> {
          // When early termination flag is set, we need the EOS block to calculate stats.
          // The upstream implementation should not send more data blocks once the early terminate signal arrives, but
          // given 2 stages between sending/receiving mailbox are setting early termination flag asynchronously,
          // there's chances that the next blocks pulled out of the ReceivingMailbox to be an already buffered normal
          // data block. This means we need to continue pulling until an EOS block is observed.
          _multiConsumer.earlyTerminate();
          TransferableBlock block = _multiConsumer.readBlockBlocking();
          while (!block.isEndOfStreamBlock()) {
            block = _multiConsumer.readBlockBlocking();
          }
        })
        .subscribeOn(_context.getIoScheduler());
  }

  private void updateStats(TransferableBlock upstreamEos) {
    _holder = upstreamEos.getQueryStats();
    for (StatMap<ReceivingMailbox.StatKey> receivingStats : _receivingStats) {
      addReceivingStats(receivingStats);
    }
  }

  private void addReceivingStats(StatMap<ReceivingMailbox.StatKey> from) {
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.RAW_MESSAGES, from.getInt(ReceivingMailbox.StatKey.DESERIALIZED_MESSAGES));
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.DESERIALIZED_BYTES, from.getLong(ReceivingMailbox.StatKey.DESERIALIZED_BYTES));
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.DESERIALIZATION_TIME_MS, from.getLong(ReceivingMailbox.StatKey.DESERIALIZATION_TIME_MS));
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.IN_MEMORY_MESSAGES, from.getInt(ReceivingMailbox.StatKey.IN_MEMORY_MESSAGES));
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.DOWNSTREAM_WAIT_MS, from.getLong(ReceivingMailbox.StatKey.OFFER_CPU_TIME_MS));
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_WAIT_MS, from.getLong(ReceivingMailbox.StatKey.WAIT_CPU_TIME_MS));
  }

  @Override
  public MultiStageQueryStats calculateStats() {
    MultiStageQueryStats holder = _holder;
    Preconditions.checkArgument(holder.getCurrentStageId() == _context.getStageId(),
        "The holder's stage id should be the same as the current operator's stage id. Expected %s, got %s",
        _context.getStageId(), holder.getCurrentStageId());
    holder.getCurrentStats().addLastOperator(MultiStageOperator.Type.MAILBOX_RECEIVE, _statMap);
    return holder;
  }

  @Override
  public List<ReactiveOperator> getChildOperators() {
    return List.of();
  }

  private static Exception extractException(Map<Integer, String> exceptions) {
    // TODO: Decide how to handle multiple exceptions
    if (exceptions.size() == 1) {
      return new RuntimeException(exceptions.values().iterator().next());
    } else {
      return new RuntimeException("Multiple exceptions occurred: " + exceptions);
    }
  }
}
