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
package org.apache.pinot.integration.tests.operators;

import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.udf.test.test.UdfReporter;
import org.apache.pinot.udf.test.test.UdfTestFramework;
import org.apache.pinot.udf.test.test.UdfTestScenario;
import org.testcontainers.shaded.com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class AddFunctionTest {

  private ClusterTestBasePinotFunctionTestCluster _cluster;
  private UdfTestFramework _framework;
  private ExecutorService _executorService;

  @BeforeClass
  public void setUp() {
    _cluster = new ClusterTestBasePinotFunctionTestCluster();
    _cluster.start();

    _executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    _framework = UdfTestFramework.fromServiceLoader(_cluster, _executorService);
    _framework.startUp();
  }

  @AfterClass
  public void tearDown() {
    if (_cluster != null) {
      _cluster.close();
    }
    if (_executorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(_executorService, 10, java.util.concurrent.TimeUnit.SECONDS);
    }
  }

  @Test
  public void myTest()
      throws InterruptedException {
    Map<Udf, Map<UdfTestScenario, Map<UdfSignature, UdfTestFramework.ResultByExample>>> results = _framework.execute();
  }

  public static void main(String... args)
      throws InterruptedException {
    AddFunctionTest addFunctionTest = new AddFunctionTest();
    addFunctionTest.setUp();
    try {
      Map<Udf, Map<UdfTestScenario, Map<UdfSignature, UdfTestFramework.ResultByExample>>> results =
          addFunctionTest._framework.execute();

      UdfReporter.reportAsMarkdown(results, udf -> new OutputStream() {
        @Override
        public void write(int b) {
          System.out.write(b);
        }
      });
    } catch (Throwable e) {
      e.printStackTrace();
    } finally {
      System.exit(0);
    }
  }
}
