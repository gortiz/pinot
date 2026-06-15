#!/usr/bin/env bash
#
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
#
# Launches the Pinot plugin verifier against a built distribution. The verifier runs as a
# separate JVM with the distribution's lib/ jars on its classpath and -Dplugins.dir pointing at
# the distribution's plugins/ directory, so realm isolation is exercised exactly as it is in a
# real broker/server/controller/minion process.
#
# Usage:
#   verify-plugins.sh [DIST_DIR] [verifier flags...]
#
# DIST_DIR is the root of an assembled distribution (the directory that contains lib/ and
# plugins/). When omitted it defaults to the parent of the directory holding this script, so
# running <dist>/bin/verify-plugins.sh with no arguments verifies <dist>. All remaining
# arguments (e.g. --check, --plugin, --strict-realm, --verbose, --help) are passed through to
# org.apache.pinot.verifier.PluginVerifier unchanged.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Only the very first argument, when it is not a flag, is the distribution directory. Everything
# after it (and everything, when the first argument is already a flag) is forwarded verbatim to
# the verifier main class. This avoids mistaking a flag value (e.g. the "stream" in
# "--check stream") for the distribution directory.
DIST_DIR=""
if [[ $# -gt 0 && "$1" != -* ]]; then
  DIST_DIR="$1"
  shift
fi
# Remaining positional parameters ("$@") are now exactly the verifier flags.

if [[ -z "${DIST_DIR}" ]]; then
  DIST_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
fi

LIB_DIR="${DIST_DIR}/lib"
PLUGINS_DIR="${DIST_DIR}/plugins"

if [[ ! -d "${LIB_DIR}" ]]; then
  echo "ERROR: ${LIB_DIR} does not exist. Pass the distribution root as the first argument," >&2
  echo "       e.g. verify-plugins.sh /path/to/apache-pinot-VERSION-bin" >&2
  exit 2
fi

if [[ -n "${JAVA_HOME:-}" ]]; then
  JAVA_CMD="${JAVA_HOME}/bin/java"
else
  JAVA_CMD="java"
fi

# lib/* contains both pinot-all.jar (the shaded core, providing PluginManager) and
# pinot-plugin-verifier-VERSION.jar (the verifier itself).
exec "${JAVA_CMD}" ${JAVA_OPTS:-} \
  -Dplugins.dir="${PLUGINS_DIR}" \
  -cp "${LIB_DIR}/*" \
  org.apache.pinot.verifier.PluginVerifier "$@"
