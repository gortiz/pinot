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
package org.apache.pinot.tools.utils;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Regression tests for {@link KafkaStarterUtils}.
 *
 * <p>{@link KafkaStarterUtils#KAFKA_SERVER_STARTABLE_CLASS_NAME} and the related constants are
 * resolved at class-load time via {@code PluginManager.get().loadServices(StreamConsumerFactory.class)}.
 * These tests verify that, when {@code pinot-kafka-3.0} is on the classpath (declared as a runtime
 * dependency of pinot-tools), the connector package is discovered and the derived class name
 * constants are non-null and non-empty.
 *
 * <p>The {@code pinot-kafka-3.0} connector registers
 * {@code org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory} via the Java SPI mechanism
 * ({@code META-INF/services/org.apache.pinot.spi.stream.StreamConsumerFactory}). Accessing the
 * public constants implicitly exercises the {@code getKafkaConnectorPackageName()} private helper.
 */
public class KafkaStarterUtilsTest {

  /**
   * Verifies that the static initializer succeeds and the Kafka startable class name constant
   * is non-null and non-empty, meaning {@code PluginManager.get().loadServices(StreamConsumerFactory.class)}
   * found at least one factory from the system classpath.
   */
  @Test
  public void kafkaServerStartableClassNameIsNonNullWhenKafkaPluginIsOnClasspath() {
    assertNotNull(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME,
        "KAFKA_SERVER_STARTABLE_CLASS_NAME must not be null when pinot-kafka-3.0 is on the classpath");
    assertFalse(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME.isEmpty(),
        "KAFKA_SERVER_STARTABLE_CLASS_NAME must not be empty");
  }

  /**
   * Verifies that the derived constants contain the expected kafka30 package prefix.
   * This guards against regressions where a different (e.g., lower-priority) StreamConsumerFactory
   * is accidentally picked over the intended kafka30 one.
   */
  @Test
  public void kafkaConstantsContainExpectedKafka30PackagePrefix() {
    String expectedPackage = "org.apache.pinot.plugin.stream.kafka30";
    assertTrue(KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME.startsWith(expectedPackage),
        "KAFKA_SERVER_STARTABLE_CLASS_NAME should start with '" + expectedPackage + "' but was: "
            + KafkaStarterUtils.KAFKA_SERVER_STARTABLE_CLASS_NAME);
    assertTrue(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME.startsWith(expectedPackage),
        "KAFKA_PRODUCER_CLASS_NAME should start with '" + expectedPackage + "' but was: "
            + KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME);
    assertTrue(KafkaStarterUtils.KAFKA_STREAM_CONSUMER_FACTORY_CLASS_NAME.startsWith(expectedPackage),
        "KAFKA_STREAM_CONSUMER_FACTORY_CLASS_NAME should start with '" + expectedPackage + "' but was: "
            + KafkaStarterUtils.KAFKA_STREAM_CONSUMER_FACTORY_CLASS_NAME);
  }
}
