---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
name: run-test
description: Run a single Pinot JUnit/TestNG test class by name. Auto-detects the owning Maven module and builds the correct ./mvnw invocation, including the integration-test flags when needed.
---

# /run-test

Purpose: resolve a test class name to its Maven module and run only that test, without the user having to remember the exact `-pl`, `-Dtest`, and `-Dsurefire.failIfNoSpecifiedTests` flags. Automatically uses the right invocation for Maven 3 (requires `-am`) vs. Maven 4 (deps resolved automatically, `-am` not needed).

Usage:
- `/run-test RangeIndexTest` — single class.
- `/run-test RangeIndexTest#testSpecificMethod` — single method.
- `/run-test OfflineClusterIntegrationTest` — integration test (auto-detected, adds the required flag).

## Procedure

1. **Parse the argument.** Split on `#` into `<className>` and optional `<methodName>`. If the class name contains a dot, treat it as FQN.

2. **Locate the source file.**
   - Glob for `**/<className>.java` under the repo.
   - Prefer matches under `src/test/java/`.
   - If multiple matches, list them (with module prefixes) and ask the user which one. Do not guess.
   - If zero matches, report and stop.

3. **Find the owning module.** Walk up from the test file until you find a `pom.xml` that is not the repo root. That's the module.

3a. **Select build tool and detect Maven version.** Check in this priority order and store results as `MVN` (executable) and `MVN_MAJOR` (integer):

   ```bash
   # 1. mvnd2 — Maven Daemon v2, always Maven 4
   if command -v mvnd2 &>/dev/null; then
     MVN=mvnd2; MVN_MAJOR=4

   # 2. mvnd — check version; mvnd 2.x.x wraps Maven 4
   elif command -v mvnd &>/dev/null; then
     MVND_MAJOR=$(mvnd --version 2>/dev/null | grep -oE 'mvnd [0-9]+' | grep -oE '[0-9]+' | head -1)
     MVN=mvnd; MVN_MAJOR=${MVND_MAJOR:-3}
     # If mvnd is Maven 3 but mvn4 exists, switch to mvn4 for Maven 4 features
     if [ "${MVN_MAJOR}" -lt 4 ] && command -v mvn4 &>/dev/null; then
       MVN=mvn4; MVN_MAJOR=4
     fi

   # 3. mvn4 — explicit Maven 4 binary
   elif command -v mvn4 &>/dev/null; then
     MVN=mvn4; MVN_MAJOR=4

   # 4. ./mvnw — repo Maven wrapper (check version)
   elif [ -f ./mvnw ]; then
     MVN=./mvnw
     MVN_MAJOR=$(./mvnw --version 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 | cut -d. -f1)

   # 5. mvn — system Maven (last resort)
   else
     MVN=mvn
     MVN_MAJOR=$(mvn --version 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 | cut -d. -f1)
   fi
   ```

4. **Note on the `failIfNoSpecifiedTests` flag.** Always pass `-Dsurefire.failIfNoSpecifiedTests=false`, regardless of Maven version or test type. With Maven 3 + `-am`, Maven runs the full Surefire goal on every upstream module (e.g. `pinot-spi` → `pinot-common` → … → target module). Each of those modules invokes Surefire with the same `-Dtest=<className>` filter, and Surefire's default behaviour is to **fail the whole build** when the pattern doesn't match any test in a given module. Without this flag, the build dies at the first upstream module that doesn't happen to contain `<className>`. With Maven 4 (no `-am`), Surefire only runs on the target module so this issue doesn't arise, but the flag is harmless to include and keeps the command consistent.

   Optional: detect integration tests for reporting/warnings only. A test is an integration test if *any* of these hold:
   - The file path contains `pinot-integration-tests`.
   - The file is named `*IntegrationTest.java`, `*IT.java`, `*ClusterTest.java`, or `*EndToEndTest.java`.
   - The module is `pinot-integration-tests` or `pinot-compatibility-verifier`.

   Use this only to warn the user about expected runtime ("integration tests typically take 10–20 min"), not to alter the command.

5. **Build the command.** Use `MVN` and `MVN_MAJOR` from step 3a:

   **Maven 4** (deps resolved automatically, no `-am`):
   ```
   $MVN -pl <module> -Dtest=<className>[#<methodName>] -Dsurefire.failIfNoSpecifiedTests=false test
   ```

   **Maven 3** (requires `-am` to build upstream module JARs first):
   ```
   $MVN -pl <module> -am -Dtest=<className>[#<methodName>] -Dsurefire.failIfNoSpecifiedTests=false test
   ```
   - `-Dsurefire.failIfNoSpecifiedTests=false` is always included regardless of Maven version (see step 4).

6. **Run and report.** Print the exact command before running so the user can copy/tweak it. On failure, show the last ~60 lines of the Maven output (or the Surefire report path under `<module>/target/surefire-reports/`) so the user can jump straight to the stack trace.

## Notes

- These runs can take 2–15 minutes depending on the module and whether deps are already built. Consider `run_in_background` only if the user says so — default is foreground so they see progress.
- **Maven 3:** Never strip `-am`. The first run after a clean checkout will fail without it, because javac needs upstream dependency JARs on the classpath. If the user wants faster iteration after the first successful build, suggest adding `-o` (offline) or dropping `-am`, but don't do it automatically.
- **Maven 4:** `-am` is not needed. Maven 4 automatically resolves and builds upstream modules. First runs are faster on a cold tree than Maven 3 equivalents.
- For repeat runs of the same test, suggest `-DfailIfNoTests=false` if the first run reported "No tests were executed" — usually a typo in the class name.
- If the class is `abstract` or has no `@Test` methods (it's a base class), warn the user and suggest concrete subclasses found via grep.
