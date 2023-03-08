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
package org.apache.pinot.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.pinot.client.base.AbstractBaseStatement;
import org.apache.pinot.client.utils.DriverUtils;


public class PinotStatement extends AbstractBaseStatement {
  private static final String LIMIT_STATEMENT = "LIMIT";

  private final PinotConnection _connection;
  private final org.apache.pinot.client.Connection _session;
  private boolean _closed;
  private ResultSet _resultSet;
  private int _maxRows = 1000000;

  public PinotStatement(PinotConnection connection) {
    _connection = connection;
    _session = connection.getSession();
    _closed = false;
    _resultSet = null;
  }

  @Override
  public void close()
      throws SQLException {
    _closed = true;
  }

  @Override
  protected void validateState()
      throws SQLException {
    if (isClosed()) {
      throw new SQLException("Statement is already closed!");
    }
  }

  @Override
  public ResultSet executeQuery(String sql)
      throws SQLException {
    validateState();
    try {
      if (!DriverUtils.queryContainsLimitStatement(sql)) {
        sql += " " + LIMIT_STATEMENT + " " + _maxRows;
      }
      String enabledSql = DriverUtils.enableQueryOptions(sql, _connection.getQueryOptions());
      ResultSetGroup resultSetGroup = _session.execute(enabledSql);
      if (resultSetGroup.getResultSetCount() == 0) {
        _resultSet = PinotResultSet.empty();
        return _resultSet;
      }
      _resultSet = new PinotResultSet(resultSetGroup.getResultSet(0));
      return _resultSet;
    } catch (PinotClientException e) {
      throw new SQLException(String.format("Failed to execute query : %s", sql), e);
    }
  }

  @Override
  public boolean execute(String sql)
      throws SQLException {
    _resultSet = executeQuery(sql);
    if (_resultSet.next()) {
      _resultSet.beforeFirst();
      return true;
    } else {
      _resultSet = null;
      return false;
    }
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys)
      throws SQLException {
    return execute(sql);
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes)
      throws SQLException {
    return execute(sql);
  }

  @Override
  public boolean execute(String sql, String[] columnNames)
      throws SQLException {
    return execute(sql);
  }

  @Override
  public ResultSet getResultSet()
      throws SQLException {
    return _resultSet;
  }

  @Override
  public Connection getConnection()
      throws SQLException {
    validateState();
    return _connection;
  }

  @Override
  public int getFetchSize()
      throws SQLException {
    return _maxRows;
  }

  @Override
  public void setFetchSize(int rows)
      throws SQLException {
    _maxRows = rows;
  }

  @Override
  public int getMaxRows()
      throws SQLException {
    return _maxRows;
  }

  @Override
  public void setMaxRows(int max)
      throws SQLException {
    _maxRows = max;
  }

  @Override
  public boolean isClosed()
      throws SQLException {
    return _closed;
  }
}
