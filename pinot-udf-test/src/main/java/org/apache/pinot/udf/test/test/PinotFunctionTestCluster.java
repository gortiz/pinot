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

import java.util.Iterator;
import java.util.stream.Stream;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/// An interface for executing queries in Pinot function tests.
public interface PinotFunctionTestCluster extends AutoCloseable {

  void start();

  void addTable(Schema schema, TableConfig tableConfig);

  void addRows(String tableName, Schema schema, Stream<GenericRow> rows);

  Iterator<GenericRow> query(ExecutionContext context, String sql);

  class ExecutionContext {
    private final boolean _nullHandlingEnabled;
    private final boolean _useMultistageEngine;

    public ExecutionContext(boolean nullHandlingEnabled, boolean useMultistageEngine) {
      _nullHandlingEnabled = nullHandlingEnabled;
      _useMultistageEngine = useMultistageEngine;
    }

    public boolean isNullHandlingEnabled() {
      return _nullHandlingEnabled;
    }

    public boolean isUseMultistageEngine() {
      return _useMultistageEngine;
    }
  }
}
