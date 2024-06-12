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

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecutorServiceUtils {
  private static final String EXECUTOR_TYPE_SUFFIX = "executor";
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorServiceUtils.class);
  private static final long DEFAULT_TERMINATION_MILLIS = 30_000;

  private static final Map<String, ExecutorServiceFactory> EXECUTOR_SERVICE_FACTORY_MAP;

  static {
    EXECUTOR_SERVICE_FACTORY_MAP = ServiceLoader.load(ExecutorServiceFactory.class).stream()
            .map(ServiceLoader.Provider::get)
            .collect(Collectors.toMap(ExecutorServiceFactory::getConfigValue, Function.identity()));
  }

  private ExecutorServiceUtils() {
  }

  public static ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName) {
    return getFactory(conf, confPrefix).create(conf, confPrefix, baseName);
  }

  public static ExecutorServiceFactory getFactory(PinotConfiguration conf, String confPrefix) {
    String propName = confPrefix + "." + EXECUTOR_TYPE_SUFFIX;
    String factoryId = conf.getProperty(propName, "cached");
    ExecutorServiceFactory factory = getFactory(factoryId);
    if (factory == null) {
      throw new IllegalArgumentException("Invalid value for " + propName + ": " + factoryId + " not recognized");
    }
    return factory;
  }

  @Nullable
  public static ExecutorServiceFactory getFactory(String factoryId) {
    return EXECUTOR_SERVICE_FACTORY_MAP.get(factoryId);
  }

  /**
   * Shuts down the given executor service.
   *
   * This method blocks a default number of millis in order to wait for termination. In case the executor doesn't
   * terminate in that time, the code continues with a logging.
   *
   * @throws RuntimeException if this threads is interrupted when waiting for termination.
   */
  public static void close(ExecutorService executorService) {
    close(executorService, DEFAULT_TERMINATION_MILLIS);
  }

  /**
   * Shuts down the given executor service.
   *
   * This method blocks up to the given millis in order to wait for termination. In case the executor doesn't terminate
   * in that time, the code continues with a logging.
   *
   * @throws RuntimeException if this threads is interrupted when waiting for termination.
   */
  public static void close(ExecutorService executorService, long terminationMillis) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(terminationMillis, TimeUnit.SECONDS)) {
        List<Runnable> runnables = executorService.shutdownNow();
        LOGGER.warn("Around " + runnables.size() + " didn't finish in time after a shutdown");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public interface ExecutorServiceFactory {
    String getConfigValue();

    ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName);
  }

  @AutoService(ExecutorServiceFactory.class)
  public static class CachedExecutorServiceFactory implements ExecutorServiceFactory {
    @Override
    public String getConfigValue() {
      return "cached";
    }

    @Override
    public ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName) {
      return Executors.newCachedThreadPool(new NamedThreadFactory(baseName));
    }
  }

  @AutoService(ExecutorServiceFactory.class)
  public static class FixedExecutorServiceFactory implements ExecutorServiceFactory {
    private static final String THREADS_CONFIG_SUFFIX = "threads";
    @Override
    public String getConfigValue() {
      return "fixed";
    }

    @Override
    public ExecutorService create(PinotConfiguration conf, String confPrefix, String baseName) {
      String nThreadsConfigProp = confPrefix + "." + EXECUTOR_TYPE_SUFFIX + "." + THREADS_CONFIG_SUFFIX;
      int nThreads = conf.getProperty(nThreadsConfigProp, -1);
      if (nThreads < 0) {
        throw new IllegalArgumentException("Config " + nThreadsConfigProp + " must be a set to a positive value");
      }
      return Executors.newFixedThreadPool(nThreads, new NamedThreadFactory(baseName));
    }
  }
}
