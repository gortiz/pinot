/*
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
package org.apache.pinot.core.util;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class AppendOnlyListTest {

  @Test
  void testAddAndGetWithinSinglePage() {
    AppendOnlyList<Object[]> list = new AppendOnlyList<>();
    Object[] row1 = {1, "a"};
    Object[] row2 = {2, "b"};
    list.add(row1);
    list.add(row2);
    assertEquals(2, list.size());
    assertEquals(row1, list.get(0));
    assertEquals(row2, list.get(1));
  }

  @Test
  void testAddAndGetAcrossMultiplePages() {
    AppendOnlyList<Object[]> list = new AppendOnlyList<>();
    int pageSize = 1024; // Must match PartitionList.PAGE_SIZE
    int totalRows = pageSize * 3 + 10;
    for (int i = 0; i < totalRows; i++) {
      list.add(new Object[]{i, "row" + i});
    }
    assertEquals(totalRows, list.size());
    // Test first, last, and page boundaries
    assertEquals(new Object[]{0, "row0"}, list.get(0));
    assertEquals(new Object[]{pageSize, "row" + pageSize}, list.get(pageSize));
    assertEquals(new Object[]{totalRows - 1, "row" + (totalRows - 1)}, list.get(totalRows - 1));
  }

  @Test
  void testIsEmpty() {
    AppendOnlyList<Object[]> list = new AppendOnlyList<>();
    assertTrue(list.isEmpty());
    list.add(new Object[]{1});
    assertFalse(list.isEmpty());
  }

  @Test
  void testOutOfBounds() {
    AppendOnlyList<Object[]> list = new AppendOnlyList<>();
    list.add(new Object[]{1});
    assertThrows(IndexOutOfBoundsException.class, () -> list.get(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> list.get(1));
  }

  @Test
  void testAddNullRows() {
    AppendOnlyList<Object[]> list = new AppendOnlyList<>();
    list.add(null);
    assertNull(list.get(0));
    assertEquals(1, list.size());
  }
}

