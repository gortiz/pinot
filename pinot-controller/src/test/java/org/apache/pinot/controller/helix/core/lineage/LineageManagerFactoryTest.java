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
package org.apache.pinot.controller.helix.core.lineage;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Regression tests for {@link LineageManagerFactory#create(ControllerConf)}.
 *
 * <p>The factory uses {@link org.apache.pinot.spi.plugin.PluginManager#loadClass(String)} (realm-aware)
 * combined with {@code getConstructor(ControllerConf.class)} (public, parameterized). Tests verify:
 * <ol>
 *   <li>A known in-tree class with a public {@code ControllerConf} constructor resolves and instantiates.</li>
 *   <li>An unknown class fails with a {@link ClassNotFoundException} somewhere in the cause chain.</li>
 * </ol>
 */
public class LineageManagerFactoryTest {

  @Test
  public void createDefaultLineageManagerReturnsDefaultImpl() {
    ControllerConf conf = new ControllerConf(Collections.emptyMap());
    // Default config returns DefaultLineageManager.class.getName()
    LineageManager manager = LineageManagerFactory.create(conf);
    assertEquals(manager.getClass(), DefaultLineageManager.class);
  }

  @Test
  public void createExplicitFqcnReturnsCorrectImpl() {
    ControllerConf conf = new ControllerConf(
        Map.of(ControllerConf.LINEAGE_MANAGER_CLASS, DefaultLineageManager.class.getName()));
    LineageManager manager = LineageManagerFactory.create(conf);
    assertEquals(manager.getClass(), DefaultLineageManager.class);
  }

  @Test
  public void createUnknownClassThrowsWithClassNotFoundInCauseChain() {
    ControllerConf conf = new ControllerConf(
        Map.of(ControllerConf.LINEAGE_MANAGER_CLASS, "org.apache.pinot.does.not.ExistLineageManager"));
    try {
      LineageManagerFactory.create(conf);
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

  /**
   * Test fixture: a minimal {@link LineageManager} with a public {@code ControllerConf} constructor,
   * matching the contract that {@link LineageManagerFactory} requires via {@code getConstructor(ControllerConf.class)}.
   */
  public static class MinimalLineageManager implements LineageManager {
    public MinimalLineageManager(ControllerConf conf) {
    }

    @Override
    public void updateLineageForStartReplaceSegments(TableConfig tableConfig, String lineageEntryId,
        Map<String, String> customMap, SegmentLineage lineage) {
    }

    @Override
    public void updateLineageForEndReplaceSegments(TableConfig tableConfig, String lineageEntryId,
        Map<String, String> customMap, SegmentLineage lineage) {
    }

    @Override
    public void updateLineageForRevertReplaceSegments(TableConfig tableConfig, String lineageEntryId,
        Map<String, String> customMap, SegmentLineage lineage) {
    }

    @Override
    public void updateLineageForRetention(TableConfig tableConfig, SegmentLineage lineage, List<String> allSegments,
        List<String> segmentsToDelete, Set<String> consumingSegments) {
    }
  }

  @Test
  public void createCustomLineageManagerWithPublicParameterizedConstructorSucceeds() {
    ControllerConf conf = new ControllerConf(
        Map.of(ControllerConf.LINEAGE_MANAGER_CLASS, MinimalLineageManager.class.getName()));
    LineageManager manager = LineageManagerFactory.create(conf);
    assertEquals(manager.getClass(), MinimalLineageManager.class);
  }
}
