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
package org.apache.pinot.query.planner.partitioning;

import java.util.List;
import org.apache.pinot.core.data.table.AbstractKey;
import org.apache.pinot.core.data.table.LightweightKey;


/**
 * The {@code FieldSelectionKeySelector} simply extract a column value out from a row array {@link Object[]}.
 */
public class LightweightKeySelector implements KeySelector<Object[], AbstractKey> {
  private static final String HASH_ALGORITHM = "absHashCode";

  private int[] _columnIndicesArr;

  public LightweightKeySelector() {
  }

  public LightweightKeySelector(int columnIndex) {
    _columnIndicesArr = new int[] {columnIndex};
  }

  public LightweightKeySelector(List<Integer> columnIndices) {
    _columnIndicesArr = new int[columnIndices.size()];
    for (int i = 0; i < columnIndices.size(); i++) {
      _columnIndicesArr[i] = columnIndices.get(i);
    }
  }

  public LightweightKeySelector(int... columnIndices) {
    _columnIndicesArr = columnIndices;
  }

  @Override
  public AbstractKey getKey(Object[] input) {
    return new LightweightKey(input, _columnIndicesArr);
  }

  @Override
  public int computeHash(Object[] input) {
    // use a hashing algorithm that is agnostic to the ordering of the columns.
    // For example, computeHash(input) will be identical for both of the following
    // values of _columnIndices:
    // - [1, 2, 4]
    // - [4, 1, 2]
    // this is necessary because calcite always sorts _columnIndices for a hash
    // broadcast, which may reorder the columns differently for different sides
    // of a join
    //
    // the result of hashcode sums will follow the Irwin-Hall distribution, which
    // is notably not a normal distribution although likely acceptable in the case
    // when there are either many inputs or not many partitions
    // also see: https://en.wikipedia.org/wiki/Irwin–Hall_distribution
    // also see: https://github.com/apache/pinot/issues/9998
    //
    // TODO: consider better hashing algorithms than hashCode sum, such as XOR'ing
    int hashCode = 0;
    for (int columnIndex : _columnIndicesArr) {
      Object value = input[columnIndex];
      if (value != null) {
        hashCode += value.hashCode();
      }
    }

    // return a positive number because this is used directly to modulo-index
    return hashCode & Integer.MAX_VALUE;
  }

  @Override
  public String hashAlgorithm() {
    return HASH_ALGORITHM;
  }
}
