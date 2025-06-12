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
package org.apache.pinot.udf.test.test;

import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.udf.test.test.scenarios.PredicateUdfTestScenario;


public class UdfTestFramework {

  private static final Comparison[] COMPARISONS = new Comparison[]{
      Comparison.EQUAL,
      Comparison.BIG_DECIMAL_AS_DOUBLE,
      Comparison.NUMBER_AS_DOUBLE
  };
  private final Set<Udf> _udfs;
  private final PinotFunctionTestCluster _cluster;
  private final Set<UdfTestScenario> _scenarios;
  private final ExecutorService _executorService;

  public UdfTestFramework(Set<Udf> udfs, PinotFunctionTestCluster cluster,
      Set<UdfTestScenario> scenarios, ExecutorService executorService) {
    _udfs = udfs;
    _cluster = cluster;
    _scenarios = scenarios;
    _executorService = executorService;
  }

  public static UdfTestFramework fromServiceLoader(PinotFunctionTestCluster cluster,
      ExecutorService executorService) {
    Set<Udf> udfs = ServiceLoader.load(Udf.class).stream()
        .map(ServiceLoader.Provider::get)
//        .filter(udf -> udf.getMainFunctionName().equals("arrayElementAtInt"))
        .collect(Collectors.toSet());
    Set<UdfTestScenario> scenarios = ServiceLoader.load(UdfTestScenario.Factory.class).stream()
        .map(ServiceLoader.Provider::get)
//        .filter(factory -> factory.getClass().getDeclaringClass().equals(PredicateUdfTestScenario.class))
//        .filter(factory -> factory.getClass().getName().endsWith("WithNullHandlingFactory"))
        .map(factory -> factory.create(cluster))
        .collect(Collectors.toSet());

    return new UdfTestFramework(udfs, cluster, scenarios, executorService);
  }

  public void startUp() {
//    CompletableFuture<?>[] futures = _udfs.stream()
//        .map(udf -> CompletableFuture.runAsync(
//            () -> PinotFunctionEnvGenerator.prepareEnvironment(_cluster, udf),
//            _executorService)
//        )
//        .toArray(CompletableFuture[]::new);
//
//    CompletableFuture.allOf(futures).join();
    for (Udf udf : _udfs) {
      PinotFunctionEnvGenerator.prepareEnvironment(_cluster, udf);
    }
  }

  public Map<Udf, Map<UdfTestScenario, Map<UdfSignature, ResultByExample>>> execute()
      throws InterruptedException {
    Map<Udf, Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfTestResult>>>>> asyncResults
        = Maps.newHashMapWithExpectedSize(_udfs.size());
    for (Udf udf : _udfs) {
      Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfTestResult>>>> scenarioResults
          = Maps.newHashMapWithExpectedSize(_scenarios.size());
      for (UdfTestScenario scenario : _scenarios) {
        Set<UdfSignature> udfSignatures = udf.getExamples().keySet();
        Map<UdfSignature, Future<Map<UdfExample, UdfTestResult>>> signatureResults
            = Maps.newHashMapWithExpectedSize(udfSignatures.size());
        for (UdfSignature signature: udfSignatures) {
          signatureResults.put(signature, _executorService.submit(() -> scenario.execute(udf, signature)));
        }
        scenarioResults.put(scenario, signatureResults);
      }
      asyncResults.put(udf, scenarioResults);
    }

    return byUdf(asyncResults);
  }

  private Map<Udf, Map<UdfTestScenario, Map<UdfSignature, ResultByExample>>> byUdf(
      Map<Udf, Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfTestResult>>>>> tasks
  ) throws InterruptedException {
    Map<Udf, Map<UdfTestScenario, Map<UdfSignature, ResultByExample>>> results
        = Maps.newHashMapWithExpectedSize(tasks.size());
    for (Map.Entry<Udf, Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfTestResult>>>>> entry
        : tasks.entrySet()) {
      Udf udf = entry.getKey();
      Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfTestResult>>>> scenarioTasks = entry.getValue();
      results.put(udf, byScenario(scenarioTasks));
    }
    return results;
  }

  private Map<UdfTestScenario, Map<UdfSignature, ResultByExample>> byScenario(
      Map<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfTestResult>>>> scenarioTasks
  ) throws InterruptedException {
    Map<UdfTestScenario, Map<UdfSignature, ResultByExample>> results = Maps.newHashMapWithExpectedSize(
        scenarioTasks.size());
    for (Map.Entry<UdfTestScenario, Map<UdfSignature, Future<Map<UdfExample, UdfTestResult>>>> entry
        : scenarioTasks.entrySet()) {
      UdfTestScenario scenario = entry.getKey();
      Map<UdfSignature, Future<Map<UdfExample, UdfTestResult>>> tasks = entry.getValue();
      results.put(scenario, bySignature(tasks));
    }
    return results;
  }

  private Map<UdfSignature, ResultByExample> bySignature(
      Map<UdfSignature, Future<Map<UdfExample, UdfTestResult>>> tasks
  ) throws InterruptedException {
    Map<UdfSignature, ResultByExample> results = Maps.newHashMapWithExpectedSize(tasks.size());
    for (Map.Entry<UdfSignature, Future<Map<UdfExample, UdfTestResult>>> entry : tasks.entrySet()) {
      UdfSignature signature = entry.getKey();
      Future<Map<UdfExample, UdfTestResult>> task = entry.getValue();
      ResultByExample result = resolve(task);
      results.put(signature, result);
    }
    return results;
  }

  private ResultByExample resolve(Future<Map<UdfExample, UdfTestResult>> task)
      throws InterruptedException {
    try {
      Map<UdfExample, UdfTestResult> result = task.get();
      Map<UdfExample, Comparison> comparisons = Maps.newHashMapWithExpectedSize(result.size());
      Map<UdfExample, String> errors = Maps.newHashMapWithExpectedSize(result.size());
      for (Map.Entry<UdfExample, UdfTestResult> entry : result.entrySet()) {
        try {
          Comparison comparison = compareResult(entry.getValue());
          comparisons.put(entry.getKey(), comparison);
        } catch (Exception e) {
          errors.put(entry.getKey(), e.getMessage());
        }
      }
      return new ResultByExample.Partial(result, comparisons, errors);
    } catch (ExecutionException e) {
      if (e.getCause().getMessage().contains("Unsupported function")) {
        return new ResultByExample.Failure("Unsupported");
      }
      return new ResultByExample.Failure(e.getCause().getMessage());
    }
  }

  private Comparison compareResult(UdfTestResult result) {
    UdfExample test = result.getTest();
    Object actualResult = result.getActualResult();
    Object expectedResult = result.getExpectedResult();

    for (Comparison comparison : COMPARISONS) {
      try {
        comparison.check(expectedResult, actualResult);
        return comparison;
      } catch (AssertionError e) {
        // Continue to the next comparison if the current one fails
      }
    }
    throw new RuntimeException("Unexpected value");
  }

  private static class Coordinate {
    private final Udf _udf;
    private final UdfSignature _signature;
    private final UdfTestScenario _scenario;

    public Coordinate(Udf udf, UdfSignature signature, UdfTestScenario scenario) {
      _udf = udf;
      _signature = signature;
      _scenario = scenario;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Coordinate)) {
        return false;
      }
      Coordinate that = (Coordinate) o;
      return Objects.equals(_udf, that._udf) && Objects.equals(_signature, that._signature)
          && Objects.equals(_scenario, that._scenario);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_udf, _signature, _scenario);
    }
  }

  public enum Comparison {
    EQUAL,
    BIG_DECIMAL_AS_DOUBLE,
    NUMBER_AS_DOUBLE,
    ERROR;

    public void check(@Nullable Object expected, @Nullable Object actual) throws AssertionError {
      switch (this) {
        case EQUAL:
          if (!Objects.equals(expected, actual)) {
            throw new AssertionError(describeDiscrepancy(expected, actual));
          }
          break;
        case NUMBER_AS_DOUBLE:
          if (expected == null && actual == null) {
            return; // Both are null, considered equal
          }
          if (expected instanceof BigDecimal && actual instanceof String) {
            // Big decimals are sent as strings through the wire protocol, so we need to convert them
            try {
              actual = new BigDecimal((String) actual);
            } catch (NumberFormatException e) {
              throw new AssertionError("Expected a number as actual value but got the String: " + actual);
            }
          }
          if (!(expected instanceof Number) || !(actual instanceof Number)) {
            Class<?> expectedClass = expected != null ? expected.getClass() : null;
            Class<?> actualClass = actual != null ? actual.getClass() : null;
            throw new AssertionError("Both expected and actual should be numbers for NUMBER_AS_DOUBLE "
                + "comparison, but got: expected=" + expected + "(of type " + expectedClass + ")"
                + ", actual=" + actual + "(of type " + actualClass + ")");
          }
          if (Double.compare(((Number) expected).doubleValue(), ((Number) actual).doubleValue()) != 0) {
            throw new AssertionError(describeDiscrepancy(expected, actual));
          }
          break;
        case BIG_DECIMAL_AS_DOUBLE: {
          if (expected instanceof BigDecimal) {
            NUMBER_AS_DOUBLE.check(expected, actual);
          } else {
            EQUAL.check(expected, actual);
          }
          break;
        }
        case ERROR:
          throw new UnsupportedOperationException("Comparison type ERROR is not supported");
        default:
          throw new IllegalArgumentException("Unknown comparison type: " + this);
      }
    }

    private static String describeDiscrepancy(@Nullable Object expected, @Nullable Object actual) {
      String expectedDesc = expected == null ? "null" : expected + " (" + expected.getClass().getSimpleName() + ")";
      String actualDesc = actual == null ? "null" : actual + " (" + actual.getClass().getSimpleName() + ")";
      return "Expected: " + expectedDesc + ", but got: " + actualDesc;
    }
  }

  public static abstract class ResultByExample {
    public static class Partial extends ResultByExample {
      private final Map<UdfExample, UdfTestResult> _resultsByExample;
      private final Map<UdfExample, Comparison> _comparisonsByExample;
      private final Map<UdfExample, String> _errorsByExample;

      public Partial(Map<UdfExample, UdfTestResult> resultsByExample, Map<UdfExample, Comparison> comparisonsByExample,
          Map<UdfExample, String> errorsByExample) {
        _resultsByExample = resultsByExample;
        _comparisonsByExample = comparisonsByExample;
        _errorsByExample = errorsByExample;
      }

      public Map<UdfExample, UdfTestResult> getResultsByExample() {
        return _resultsByExample;
      }

      public Map<UdfExample, Comparison> getComparisonsByExample() {
        return _comparisonsByExample;
      }

      public Map<UdfExample, String> getErrorsByExample() {
        return _errorsByExample;
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof Partial)) {
          return false;
        }
        Partial partial = (Partial) o;
        return Objects.equals(getResultsByExample(), partial.getResultsByExample()) && Objects.equals(
            getComparisonsByExample(), partial.getComparisonsByExample()) && Objects.equals(getErrorsByExample(),
            partial.getErrorsByExample());
      }

      @Override
      public int hashCode() {
        return Objects.hash(getResultsByExample(), getComparisonsByExample(), getErrorsByExample());
      }
    }

    public static class Failure extends ResultByExample {
      private final String _errorMessage;

      public Failure(String errorMessage) {
        _errorMessage = errorMessage;
      }

      public String getErrorMessage() {
        return _errorMessage;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) {
          return true;
        }
        if (!(o instanceof ResultByExample.Failure)) {
          return false;
        }
        Failure that = (Failure) o;
        return _errorMessage.equals(that._errorMessage);
      }

      @Override
      public int hashCode() {
        return _errorMessage.hashCode();
      }
    }
  }
}
