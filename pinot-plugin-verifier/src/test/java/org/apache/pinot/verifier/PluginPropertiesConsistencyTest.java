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
package org.apache.pinot.verifier;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;


/**
 * Guards consistency of {@code pinot-plugin.properties} files across all plugin modules under
 * {@code pinot-plugins/}.
 *
 * <p>The branch that introduced the no-shading layout added ~25 copies of this file. Most are
 * intentionally byte-identical boilerplate ({@code parent.realmId=pinot}). A small number differ
 * for documented reasons and are captured in {@link #EXEMPT_RELATIVE_PATHS}. This test prevents
 * silent drift: if someone edits the canonical copy without updating the others, or vice-versa,
 * the test fails with a clear message listing every divergent file.
 *
 * <p>The test locates the repository root via the {@code pinot.root} system property, which is
 * injected by the module's Surefire configuration. If the property is absent or the
 * {@code pinot-plugins} directory cannot be found (indicating a partial CI checkout), the test
 * throws {@link SkipException}. If the directory exists but consistency is violated, the test
 * throws {@link AssertionError}.
 */
public class PluginPropertiesConsistencyTest {

  /**
   * Canonical reference file — pinot-kafka-3.0 was chosen because it is the primary real-time
   * ingestion plugin and is representative of the standard {@code parent.realmId=pinot} form.
   * Paths use forward slashes for cross-platform consistency.
   */
  static final String CANONICAL_RELATIVE_PATH =
      "pinot-plugins/pinot-stream-ingestion/pinot-kafka-3.0/src/main/resources/pinot-plugin.properties";

  /**
   * Module-relative paths (relative to the repository root, always with forward slashes) that are
   * intentionally allowed to differ from the canonical file. Each entry must include a comment
   * explaining why it is exempt.
   *
   * <ul>
   *   <li>{@code pinot-plugins/pinot-metrics/pinot-dropwizard/src/main/resources/pinot-plugin.properties}
   *       — Dropwizard is deliberately a <em>limited-realm</em> plugin: it ships its own Metrics
   *       jars and must not share the parent realm's Metrics classes to avoid version conflicts.
   *       It therefore omits {@code parent.realmId=pinot}.</li>
   *   <li>{@code pinot-plugins/assembly-descriptor/src/it/projects/simple-assembly/src/main/resources/}
   *       {@code pinot-plugin.properties} — This is a Maven Invoker test-fixture project used to
   *       exercise the assembly-descriptor Maven plugin. It deliberately uses a different
   *       {@code importFrom.pinot} form to verify the assembler handles non-standard files.
   *       It is not a real plugin.</li>
   * </ul>
   */
  static final Set<String> EXEMPT_RELATIVE_PATHS = new HashSet<>(Arrays.asList(
      // Dropwizard intentionally omits parent.realmId to stay in its own limited realm and avoid
      // Metrics version conflicts with the parent classloader.
      "pinot-plugins/pinot-metrics/pinot-dropwizard/src/main/resources/pinot-plugin.properties",

      // Maven Invoker test fixture for the assembly-descriptor plugin — not a real plugin, uses a
      // different property form to exercise the assembler with a non-standard file.
      "pinot-plugins/assembly-descriptor/src/it/projects/simple-assembly/src/main/resources/pinot-plugin.properties"
  ));

  @Test
  public void testPluginPropertiesConsistency()
      throws IOException {
    Path repoRoot = resolveRepoRoot();
    checkConsistency(repoRoot);
  }

  @Test
  public void testDetectsDivergentFile()
      throws IOException {
    // Build a minimal fake checkout: one canonical file and one divergent copy.
    Path root = Files.createTempDirectory("pinot-consistency-detect-test");
    try {
      Path canonical = root.resolve(CANONICAL_RELATIVE_PATH);
      Files.createDirectories(canonical.getParent());
      Files.writeString(canonical, "parent.realmId=pinot\n");

      // A non-exempt module with a different value — must be detected.
      Path divergent = root.resolve(
          "pinot-plugins/pinot-input-format/pinot-csv/src/main/resources/pinot-plugin.properties");
      Files.createDirectories(divergent.getParent());
      Files.writeString(divergent, "parent.realmId=WRONG\n");

      try {
        checkConsistency(root);
        Assert.fail("Expected AssertionError for divergent file, but none was thrown");
      } catch (AssertionError e) {
        Assert.assertTrue(e.getMessage().contains("pinot-csv"),
            "Expected error message to name the divergent module, got: " + e.getMessage());
      }
    } finally {
      deleteTree(root);
    }
  }

  @Test
  public void testExemptFilesAreNotFlagged()
      throws IOException {
    // Build a minimal fake checkout where only exempt files differ — must pass cleanly.
    Path root = Files.createTempDirectory("pinot-consistency-exempt-test");
    try {
      Path canonical = root.resolve(CANONICAL_RELATIVE_PATH);
      Files.createDirectories(canonical.getParent());
      Files.writeString(canonical, "parent.realmId=pinot\n");

      for (String exemptPath : EXEMPT_RELATIVE_PATHS) {
        Path exemptFile = root.resolve(exemptPath);
        Files.createDirectories(exemptFile.getParent());
        Files.writeString(exemptFile, "# intentionally different\n");
      }

      // Must not throw — all divergent files are in the exempt set.
      checkConsistency(root);
    } finally {
      deleteTree(root);
    }
  }

  /**
   * Core consistency check, separated from the test method so that both the real-repo test and
   * the synthetic negative tests can invoke it directly.
   *
   * @param repoRoot the repository root containing {@code pinot-plugins/}
   * @throws IOException if any file cannot be read
   * @throws AssertionError if any non-exempt file differs from the canonical
   * @throws SkipException if the canonical file or plugins directory cannot be found (partial checkout)
   */
  static void checkConsistency(Path repoRoot)
      throws IOException {
    Path pluginsDir = repoRoot.resolve("pinot-plugins");
    if (!Files.isDirectory(pluginsDir)) {
      throw new SkipException(
          "pinot-plugins directory not found under repo root " + repoRoot
              + " — skipping (partial checkout).");
    }

    Path canonicalFile = repoRoot.resolve(CANONICAL_RELATIVE_PATH);
    if (!Files.exists(canonicalFile)) {
      throw new AssertionError(
          "Canonical pinot-plugin.properties not found at " + canonicalFile
              + ". If the module was renamed or moved, update CANONICAL_RELATIVE_PATH in "
              + PluginPropertiesConsistencyTest.class.getSimpleName() + ".");
    }
    byte[] canonicalBytes = Files.readAllBytes(canonicalFile);

    PathMatcher matcher = pluginsDir.getFileSystem()
        .getPathMatcher("glob:**/src/main/resources/pinot-plugin.properties");

    List<String> failures = new ArrayList<>();
    boolean[] canonicalSeen = {false};
    try (Stream<Path> walk = Files.walk(pluginsDir)) {
      walk.filter(p -> !hasPathSegment(pluginsDir.relativize(p), "target"))
          .filter(matcher::matches)
          .sorted()
          .forEach(candidate -> {
            // Use forward slashes in the relative path for cross-platform Set membership checks.
            String relPath = repoRoot.relativize(candidate).toString().replace('\\', '/');
            if (relPath.equals(CANONICAL_RELATIVE_PATH)) {
              canonicalSeen[0] = true;
            }
            if (EXEMPT_RELATIVE_PATHS.contains(relPath)) {
              return;
            }
            try {
              byte[] candidateBytes = Files.readAllBytes(candidate);
              if (!Arrays.equals(canonicalBytes, candidateBytes)) {
                failures.add(relPath);
              }
            } catch (IOException e) {
              throw new UncheckedIOException("Failed to read " + candidate, e);
            }
          });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }

    if (!canonicalSeen[0]) {
      throw new AssertionError(
          "Canonical file was not encountered during the walk: " + CANONICAL_RELATIVE_PATH
              + ". Check that the glob pattern and CANONICAL_RELATIVE_PATH are consistent.");
    }

    if (!failures.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("The following pinot-plugin.properties files differ from the canonical copy at\n");
      sb.append("  ").append(CANONICAL_RELATIVE_PATH).append("\n\n");
      sb.append("Either update these files to match the canonical, add them to EXEMPT_RELATIVE_PATHS\n");
      sb.append("with an explanation, or update the canonical itself (and fix all copies):\n\n");
      for (String path : failures) {
        sb.append("  ").append(path).append("\n");
      }
      throw new AssertionError(sb.toString());
    }
  }

  /**
   * Resolves the repository root. Uses the {@code pinot.root} system property injected by
   * Surefire. Falls back to walking up from {@code user.dir} looking for a {@code pom.xml} that
   * also has a {@code pinot-plugins} sibling (a reliable marker of the Pinot multi-module root).
   *
   * @return the repository root path
   * @throws SkipException if the root cannot be located (e.g. partial CI checkout or misconfigured
   *     Surefire property)
   */
  static Path resolveRepoRoot() {
    String pinotRootProp = System.getProperty("pinot.root");
    if (pinotRootProp != null && !pinotRootProp.isEmpty() && !pinotRootProp.startsWith("$")) {
      Path candidate = Path.of(pinotRootProp).toAbsolutePath().normalize();
      if (Files.isDirectory(candidate.resolve("pinot-plugins"))) {
        return candidate;
      }
      // The property was set but points to the wrong location — fail loudly rather than
      // silently falling through to the heuristic, which could validate the wrong tree.
      throw new SkipException(
          "pinot.root sysprop resolved to '" + candidate
              + "' but it does not contain pinot-plugins/. "
              + "Check the Surefire systemPropertyVariables in pinot-plugin-verifier/pom.xml.");
    }

    // Fallback: walk up from user.dir looking for the repo root marker.
    Path dir = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
    for (int depth = 0; depth < 6; depth++) {
      if (Files.exists(dir.resolve("pom.xml")) && Files.isDirectory(dir.resolve("pinot-plugins"))) {
        return dir;
      }
      Path parent = dir.getParent();
      if (parent == null) {
        break;
      }
      dir = parent;
    }

    throw new SkipException(
        "Cannot locate the Pinot repository root (need a directory containing both pom.xml and"
            + " pinot-plugins/). Set -Dpinot.root=<repoRoot> or run from within the checkout."
            + " pinot.root sysprop was: " + pinotRootProp);
  }

  /**
   * Returns {@code true} if any element of path {@code p} equals {@code segment} exactly.
   * Used to exclude {@code target/} build-output directories from the file walk.
   * The path should be relative (e.g. relativized from the plugins directory) to avoid
   * accidental matches on parent directory names.
   */
  private static boolean hasPathSegment(Path p, String segment) {
    for (int i = 0; i < p.getNameCount(); i++) {
      if (p.getName(i).toString().equals(segment)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Recursively deletes a temporary directory tree. Used to clean up after synthetic tests.
   */
  private static void deleteTree(Path root)
      throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(root)) {
      walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
        try {
          Files.delete(p);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }
}
