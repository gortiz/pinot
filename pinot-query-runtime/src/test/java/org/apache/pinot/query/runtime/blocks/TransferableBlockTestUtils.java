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
package org.apache.pinot.query.runtime.blocks;

import org.apache.pinot.common.datablock.DataBlock;
import org.testng.Assert;


public class TransferableBlockTestUtils {
  private TransferableBlockTestUtils() {
    // do not instantiate.
  }

  public static TransferableBlock getEndOfStreamTransferableBlock() {
    return TransferableBlockUtils.getEndOfStreamTransferableBlock(0);
  }

  public static void assertSuccessEos(TransferableBlock block) {
    Assert.assertEquals(block.getType(), DataBlock.Type.METADATA, "Block type should be metadata");
    Assert.assertTrue(block.isSuccessfulEndOfStreamBlock(), "Block should be successful EOS");
  }

  public static void assertDataBlock(TransferableBlock block) {
    if (block.getType() != DataBlock.Type.ROW && block.getType() != DataBlock.Type.COLUMNAR) {
      Assert.fail("Block type should be row or columnar but found " + block.getType());
    }
  }
}