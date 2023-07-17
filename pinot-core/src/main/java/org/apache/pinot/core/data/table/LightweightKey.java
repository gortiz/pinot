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
package org.apache.pinot.core.data.table;

import java.util.Arrays;
import java.util.Objects;

public class LightweightKey extends AbstractKey {

  private final Object[] _row;
  private final int[] _columnIndices;

  public LightweightKey(Object[] row, int[] columnIndices) {
    _row = row;
    _columnIndices = columnIndices;
  }

  @Override
  public Object[] getValues() {
    Object[] key = new Object[_columnIndices.length];
    for (int i = 0; i < _columnIndices.length; i++) {
      int position = _columnIndices[i];
      key[i] = _row[position];
    }
    return key;
  }

  @Override
  public int hashCode() {
    int result = 1;

    for (int columnIndex : _columnIndices) {
      Object element = _row[columnIndex];
      result = 31 * result + (element == null ? 0 : element.hashCode());
    }

    return result;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);
  }

  public boolean lightweightEquals(AbstractKey other) {
    if (other instanceof LightweightKey) {
      LightweightKey lightweightOther = (LightweightKey) other;
      if (!Arrays.equals(_columnIndices, lightweightOther._columnIndices)) {
        return false;
      }
      for (int position : _columnIndices) {
        if (!Objects.equals(_row[position], lightweightOther._row[position])) {
          return false;
        }
      }
      return true;
    }
    Object[] otherValues = other.getValues();
    for (int i = 0; i < _columnIndices.length; i++) {
      int position = _columnIndices[i];
      if (!_row[position].equals(otherValues[i])) {
        return false;
      }
    }
    return true;
  }
}
