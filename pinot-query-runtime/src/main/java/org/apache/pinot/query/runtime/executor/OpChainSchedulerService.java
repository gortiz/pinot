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
package org.apache.pinot.query.runtime.executor;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.pinot.core.util.trace.TraceRunnable;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OpChainId;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.trace.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Schedulers;


public class OpChainSchedulerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpChainSchedulerService.class);

  private final ExecutorService _executorService;
  private final ConcurrentHashMap<OpChainId, Future<?>> _submittedOpChainMap;

  public OpChainSchedulerService(ExecutorService executorService) {
    _executorService = executorService;
    _submittedOpChainMap = new ConcurrentHashMap<>();
  }

  public void register(OpChain operatorChain) {
    CompletableFuture<?> scheduledFuture = operatorChain.getRoot().toFlux()
        .doFirst(() -> {
          ThreadResourceUsageProvider threadResourceUsageProvider = new ThreadResourceUsageProvider();
          Tracing.ThreadAccountantOps.setupWorker(operatorChain.getId().getStageId(),
              ThreadExecutionContext.TaskType.MSE, threadResourceUsageProvider,
              operatorChain.getParentContext());
          LOGGER.trace("({}): Executing", operatorChain);
        })
        .subscribeOn(Schedulers.fromExecutorService(_executorService))
        .ignoreElements()
        .doOnSuccess(ignoreMe -> { // ignoreMe will be null given we ignore values above
          MultiStageQueryStats stats = operatorChain.getRoot().calculateStats();
          LOGGER.debug("({}): Completed {}", operatorChain, stats);
        })
        .doOnError(throwable -> {
          LOGGER.error("({}): Completed erroneously", operatorChain, throwable);
        })
        .doAfterTerminate(() -> {
          _submittedOpChainMap.remove(operatorChain.getId());
          operatorChain.close();
          Tracing.ThreadAccountantOps.clear();
        })
        .subscribeOn(Schedulers.fromExecutorService(_executorService))
        .toFuture();

    _submittedOpChainMap.put(operatorChain.getId(), scheduledFuture);
  }

  public void cancel(long requestId) {
    // simple cancellation. for leaf stage this cannot be a dangling opchain b/c they will eventually be cleared up
    // via query timeout.
    Iterator<Map.Entry<OpChainId, Future<?>>> iterator = _submittedOpChainMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<OpChainId, Future<?>> entry = iterator.next();
      if (entry.getKey().getRequestId() == requestId) {
        entry.getValue().cancel(true);
        iterator.remove();
      }
    }
  }
}
