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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;


public class UdfReporter {

  private UdfReporter() {
  }

  /// Gets the result of running the UDF test framework on a set of UDFs and scenarios and generate a markdown report
  ///  of the results.
  ///
  /// The markdown report will contain a h2 for each suite and a table.
  /// The table will be like:
  /// | Scenario | Signature | Call | Expected result | Actual result | Comparison or Error |
  /// |----------|-----------|------|-----------------|---------------|---------------------|
  ///
  /// And will have one row for each signature in the suite.
  ///
  /// The way both results are printed is `value (java type)` when types are important.
  /// Comparison or Error will be either a comparison (as defined in
  /// [UdfTestFramework.Comparison])
  public static void reportAsMarkdown(
      Map<Udf, Map<UdfTestScenario, Map<UdfSignature, UdfTestFramework.ResultByExample>>> results,
      Function<Udf, OutputStream> osGenerator
  )
      throws IOException {

    TreeSet<Udf> udfs = new TreeSet<>(Comparator.comparing(Udf::getMainFunctionName));
    udfs.addAll(results.keySet());
    for (Udf udf : udfs) {
      try (OutputStream os = osGenerator.apply(udf);
          PrintWriter report = new PrintWriter(os, false)) {
        report.append("## ").append(udf.getMainFunctionName()).append("\n\n");
        TreeSet<UdfTestScenario> scenarios = new TreeSet<>(Comparator.comparing(UdfTestScenario::getTitle));

        Map<UdfTestScenario, Map<UdfSignature, UdfTestFramework.ResultByExample>> byScenario = results.get(udf);

        scenarios.addAll(byScenario.keySet());

        SortedMap<UdfTestScenario, String> summaries = new TreeMap<>(Comparator.comparing(UdfTestScenario::getTitle));
        for (UdfTestScenario scenario : scenarios) {
          Map<UdfSignature, UdfTestFramework.ResultByExample> bySignature = byScenario.get(scenario);
          summaries.put(scenario, summarize(bySignature));
        }
        if (summaries.values().stream().distinct().count() == 1) {
          String summary = summaries.values().iterator().next();
          if (summary.equals("❌ Unsupported")) {
            report.append("### Summary\n\n")
                .append("The UDF ").append(udf.getMainFunctionName())
                .append(" is not supported in all scenarios.\n\n");
          } else if (summary.contains("❌")) {
            report.append("### Summary\n\n")
                .append("The UDF ").append(udf.getMainFunctionName())
                .append(" has failed in all scenarios with the following error: ")
                .append(summary).append("\n\n");
          } else {
            report.append("### Summary\n\n")
                .append("The UDF ").append(udf.getMainFunctionName())
                .append(" is supported in all scenarios with at least ")
                .append(summary).append(" semantic.\n\n");
          }
        } else {
          report.append("| Scenario | Semantic |\n");
          report.append("|----------|----------|\n");

          for (Map.Entry<UdfTestScenario, String> entry : summaries.entrySet()) {
            report.append("| ").append(entry.getKey().getTitle()).append(" | ")
                .append(entry.getValue()).append(" |\n");
          }
        }

        report.append("### Details\n\n");

        for (UdfTestScenario scenario : scenarios) {
          Map<UdfSignature, UdfTestFramework.ResultByExample> bySignature = byScenario.get(scenario);

          TreeSet<UdfSignature> signatures = new TreeSet<>(Comparator.comparing(UdfSignature::toString));
          signatures.addAll(bySignature.keySet());

          report.append("#### ").append(scenario.getTitle()).append("\n\n");

          report.append('\n');

          report.append("| Signature | Call | Expected result | Actual result | Comparison or Error |\n");
          report.append("|-----------|------|-----------------|---------------|---------------------|\n");

          for (UdfSignature signature : signatures) {
            UdfTestFramework.ResultByExample resultByExample = bySignature.get(signature);

            if (resultByExample instanceof UdfTestFramework.ResultByExample.Partial) {
              UdfTestFramework.ResultByExample.Partial partial = (UdfTestFramework.ResultByExample.Partial) resultByExample;
              for (Map.Entry<UdfExample, UdfTestResult> exampleEntry : partial.getResultsByExample().entrySet()) {
                UdfExample example = exampleEntry.getKey();
                UdfTestResult testResult = exampleEntry.getValue();

                // Signature column
                report.append("| ")
                    .append(signature.toString()).append(" |");

                // Call column
                report.append(asSqlCallWithLiteralArgs(udf, udf.getMainFunctionName(), example.getInputValues()))
                    .append(" |");

                // Expected result
                Object expected = testResult.getExpectedResult();
                Object actual = testResult.getActualResult();

                Function<Object, String> valueFormatter = getResultFormatter(expected, actual);
                report.append(valueFormatter.apply(expected)).append(" |")
                    .append(valueFormatter.apply(actual)).append(" |");

                // Comparison or Error
                String error = partial.getErrorsByExample().get(example);
                if (error != null) {
                  report.append("❌ ").append(error.replace("\n", " ")).append(" |\n");
                } else {
                  UdfTestFramework.Comparison comparison = partial.getComparisonsByExample().get(example);
                  report.append(comparison != null ? comparison.name() : "").append(" |\n");
                }
              }
            } else if (resultByExample instanceof UdfTestFramework.ResultByExample.Failure) {
              UdfTestFramework.ResultByExample.Failure failure = (UdfTestFramework.ResultByExample.Failure) resultByExample;

              report.append("| ")
                  .append(signature.toString())
                  .append(" | - | - | - | ❌ ")
                  .append(failure.getErrorMessage().replace("\n", " "))
                  .append(" |\n");
            }
          }
        }
        report.flush();
      }
    }
  }

  private static String summarize(Map<UdfSignature, UdfTestFramework.ResultByExample> bySignature) {

    UdfTestFramework.Comparison comparison = UdfTestFramework.Comparison.EQUAL;
    boolean withSuccess = false;
    int errors = 0;
    for (UdfTestFramework.ResultByExample result : bySignature.values()) {
      if (result instanceof UdfTestFramework.ResultByExample.Failure) {
        String error = (((UdfTestFramework.ResultByExample.Failure) result)).getErrorMessage().replace("\n", " ");
        return "❌ " + error;
      }
      if (result instanceof UdfTestFramework.ResultByExample.Partial) {
        UdfTestFramework.ResultByExample.Partial partial = (UdfTestFramework.ResultByExample.Partial) result;

        withSuccess |= !partial.getComparisonsByExample().isEmpty();
        for (UdfTestFramework.Comparison value : partial.getComparisonsByExample().values()) {
          if (value.compareTo(comparison) > 0) {
            comparison = value;
          }
        }

        errors += partial.getErrorsByExample().values().size();
      }
    }

    if (withSuccess) {
      if (errors == 0) {
        return comparison.name();
      }
      return comparison.name() + " with " + errors + " errors.";
    } else {
      return "Not supported";
    }
  }

  private static Function<Object, String> getResultFormatter(Object expected, Object actual) {
    Function<Object, String> valueFormatter;
    if (expected != null && actual != null && expected.getClass().equals(actual.getClass())) {
      valueFormatter = value -> {
        if (value.getClass().isArray()) {
          return Arrays.toString((Object[]) value);
        }
        return value.toString();
      };
    } else {
      valueFormatter = value -> {
        if (value == null) {
          return "NULL";
        } else if (value.getClass().isArray()) {
          return Arrays.toString((Object[]) value)
              + " ( array of " + value.getClass().getComponentType().getSimpleName() + ")";
        } else {
          return value + " (" + value.getClass().getSimpleName() + ")";
        }
      };
    }
    return valueFormatter;
  }

  private static String asSqlCallWithLiteralArgs(Udf udf, String name, List<Object> inputs) {
    List<String> args = inputs.stream().map(o -> {
      if (o == null) {
        return "NULL";
      } else if (o.getClass().isArray()) {
        return Arrays.toString((Object[]) o);
      } else {
        return o.toString();
      }
    }).collect(Collectors.toList());
    return udf.asSqlCall(name, args);
  }
}
