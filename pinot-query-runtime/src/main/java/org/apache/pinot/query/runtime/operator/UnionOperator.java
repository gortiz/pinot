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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Union operator for UNION ALL queries.
 */
public class UnionOperator extends SetOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnionOperator.class);
  private static final String EXPLAIN_NAME = "UNION";
  @Nullable
  private MultiStageQueryStats _queryStats = null;

  public UnionOperator(OpChainExecutionContext opChainExecutionContext, List<MultiStageOperator<?>> upstreamOperators,
      DataSchema dataSchema) {
    super(opChainExecutionContext, upstreamOperators, dataSchema);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public Type getOperatorType() {
    return Type.UNION;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    for (MultiStageOperator<?> upstreamOperator : getChildOperators()) {
      TransferableBlock block = upstreamOperator.nextBlock();
      if (!block.isEndOfStreamBlock()) {
        return block;
      } else {
        MultiStageQueryStats queryStats = block.getQueryStats();
        assert queryStats != null;
        if (_queryStats == null) {
          Preconditions.checkArgument(queryStats.getCurrentStageId() == _context.getStageId(),
              "The current stage id of the stats holder: %s does not match the current stage id: %s",
              queryStats.getCurrentStageId(), _context.getStageId());
          _queryStats = queryStats;
        } else {
          _queryStats.mergeUpstream(queryStats);
        }
      }
    }
    assert _queryStats != null : "Should have at least one EOS block from the upstream operators";
    addStats(_queryStats);
    return TransferableBlockUtils.getEndOfStreamTransferableBlock(_queryStats);
  }

  @Override
  protected boolean handleRowMatched(Object[] row) {
    throw new UnsupportedOperationException("Union operator does not support row matching");
  }
}
