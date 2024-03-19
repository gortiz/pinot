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
package org.apache.pinot.client.base;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;


public abstract class AbstractBaseConnection implements Connection {
  protected abstract void validateState()
      throws SQLException;

  @Override
  public void abort(Executor executor) {
    // no-op
  }

  @Override
  public void clearWarnings() {
    // no-op
  }

  @Override
  public void commit() {
    // no-op
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) {
    return null;
  }

  @Override
  public Blob createBlob() {
    return null;
  }

  @Override
  public Clob createClob() {
    return null;
  }

  @Override
  public NClob createNClob() {
    return null;
  }

  @Override
  public SQLXML createSQLXML() {
    return null;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
    return null;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) {
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) {
    return null;
  }

  @Override
  public boolean getAutoCommit() {
    return false;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) {
    // no-op
  }

  @Override
  public String getCatalog() {
    return null;
  }

  @Override
  public void setCatalog(String catalog) {
    // no-op
  }

  @Override
  public String getClientInfo(String name) {
    return null;
  }

  @Override
  public Properties getClientInfo() {
    return null;
  }

  @Override
  public void setClientInfo(Properties properties) {
    // no-op
  }

  @Override
  public int getHoldability() {
    return 0;
  }

  @Override
  public void setHoldability(int holdability) {
    // no-op
  }

  @Override
  public int getNetworkTimeout() {
    return 0;
  }

  @Override
  public String getSchema() {
    return null;
  }

  @Override
  public void setSchema(String schema) {
    // no-op
  }

  @Override
  public int getTransactionIsolation() {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public void setTransactionIsolation(int level) {
    // no-op
  }

  @Override
  public Map<String, Class<?>> getTypeMap() {
    return null;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) {
    // no-op
  }

  @Override
  public SQLWarning getWarnings() {
    return null;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public void setReadOnly(boolean readOnly) {
    // no-op
  }

  @Override
  public boolean isValid(int timeout)
      throws SQLException {
    return !isClosed();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }

  @Override
  public String nativeSQL(String sql) {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql) {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) {
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) {
    return null;
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) {
    return null;
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) {
    return null;
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
    return null;
  }

  @Override
  public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes) {
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) {
    return null;
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) {
    // no-op
  }

  @Override
  public void rollback() {
    // no-op
  }

  @Override
  public void rollback(Savepoint savepoint) {
    // no-op
  }

  @Override
  public void setClientInfo(String name, String value) {
    // no-op
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) {
    // no-op
  }

  @Override
  public Savepoint setSavepoint() {
    return null;
  }

  @Override
  public Savepoint setSavepoint(String name) {
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    return null;
  }
}
