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
package org.apache.pinot.query.runtime.blocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class RowHeapDataBlockProjectionTest {
  private static final DataSchema SCHEMA = new DataSchema(new String[]{"i", "s", "l"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.LONG});
  private static final DataSchema PROJECTED = new DataSchema(new String[]{"l", "i"},
      new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.INT});

  private static RowHeapDataBlock block() {
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      rows.add(new Object[]{i, "s" + i, i * 10L});
    }
    return new RowHeapDataBlock(rows, SCHEMA);
  }

  @Test
  public void viewHoldsTheSelectedColumns() {
    RowHeapDataBlock view = block().project(new int[]{2, 0}, PROJECTED);
    assertEquals(view.getNumRows(), 5);
    assertSame(view.getDataSchema(), PROJECTED);
    List<Object[]> rows = view.getRows();
    for (int i = 0; i < 5; i++) {
      assertEquals(rows.get(i), new Object[]{i * 10L, i});
    }
    // The rows are created once.
    assertSame(view.getRows(), rows);
  }

  /// The view serializes from the source rows. The result must be the same as serializing the projected rows.
  @Test
  public void viewSerializesLikeTheProjectedRows() {
    RowHeapDataBlock view = block().project(new int[]{2, 0}, PROJECTED);
    DataBlock serialized = view.asSerialized().getDataBlock();
    DataBlock expected = new RowHeapDataBlock(view.getRows(), PROJECTED).asSerialized().getDataBlock();
    assertRows(DataBlockExtractUtils.extractRows(serialized), DataBlockExtractUtils.extractRows(expected));
    assertEquals(serialized.getDataSchema(), PROJECTED);
  }

  @Test
  public void projectionOfAViewReadsTheOriginalRows() {
    RowHeapDataBlock view = block().project(new int[]{2, 0}, PROJECTED);
    DataSchema onlyI = new DataSchema(new String[]{"i"}, new ColumnDataType[]{ColumnDataType.INT});
    RowHeapDataBlock viewOfView = view.project(new int[]{1}, onlyI);
    List<Object[]> rows = viewOfView.getRows();
    for (int i = 0; i < 5; i++) {
      assertEquals(rows.get(i), new Object[]{i});
    }
    assertRows(DataBlockExtractUtils.extractRows(viewOfView.asSerialized().getDataBlock()), rows);
  }

  @Test
  public void invalidProjectionsAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> block().project(new int[]{3, 0}, PROJECTED));
    assertThrows(IllegalArgumentException.class, () -> block().project(new int[]{0}, PROJECTED));
  }

  private static void assertRows(List<Object[]> actual, List<Object[]> expected) {
    assertTrue(Arrays.deepEquals(actual.toArray(), expected.toArray()),
        Arrays.deepToString(actual.toArray()) + " != " + Arrays.deepToString(expected.toArray()));
  }
}
