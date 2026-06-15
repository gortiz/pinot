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
package org.apache.pinot.controller.helix.core.rebalance;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Regression tests for {@link RebalancePreCheckerFactory#create(String)}.
 *
 * <p>The factory is realm-aware via {@link org.apache.pinot.spi.plugin.PluginManager#loadClass(String)}
 * combined with {@code getDeclaredConstructor().newInstance()}.
 */
public class RebalancePreCheckerFactoryTest {

  @Test
  public void createDefaultRebalancePreCheckerReturnsDefaultImpl() {
    RebalancePreChecker checker = RebalancePreCheckerFactory.create(DefaultRebalancePreChecker.class.getName());
    assertEquals(checker.getClass(), DefaultRebalancePreChecker.class);
  }

  @Test
  public void createUnknownClassThrowsWithClassNotFoundInCauseChain() {
    try {
      RebalancePreCheckerFactory.create("org.apache.pinot.does.not.ExistPreChecker");
      fail("Expected a RuntimeException for unknown class");
    } catch (RuntimeException thrown) {
      boolean hasClassNotFound = false;
      Throwable t = thrown;
      while (t != null) {
        if (t instanceof ClassNotFoundException || t instanceof NoClassDefFoundError) {
          hasClassNotFound = true;
          break;
        }
        t = t.getCause();
      }
      assertTrue(hasClassNotFound,
          "Expected ClassNotFoundException in the cause chain, got: " + thrown);
    }
  }
}
