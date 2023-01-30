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
package org.apache.pinot.segment.local.segment.creator.impl.nullvalue;

import com.google.common.annotations.VisibleForTesting;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Used to persist the null bitmap on disk. This is used by SegmentCreator while indexing rows.
 */
public class NullValueVectorCreator implements IndexCreator {
  private final MutableRoaringBitmap _nullBitmap = new MutableRoaringBitmap();
  private final File _nullValueVectorFile;

  @Override
  public void addSingleValueCell(@Nonnull Object value, int dictId, Object alternative)
      throws IOException {
    throw new UnsupportedOperationException("NullValueVector should not be build as a normal index");
  }

  @Override
  public void addMultiValueCell(@Nonnull Object[] values, @Nullable int[] dictIds, Object[] alternative)
      throws IOException {
    throw new UnsupportedOperationException("NullValueVector should not be build as a normal index");
  }

  public NullValueVectorCreator(File indexDir, String columnName) {
    _nullValueVectorFile = new File(indexDir, columnName + V1Constants.Indexes.NULLVALUE_VECTOR_FILE_EXTENSION);
  }

  public void setNull(int docId) {
    _nullBitmap.add(docId);
  }

  public void seal()
      throws IOException {
    // Create null value vector file only if the bitmap is not empty
    if (!_nullBitmap.isEmpty()) {
      try (DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(_nullValueVectorFile))) {
        _nullBitmap.serialize(outputStream);
      }
    }
  }

  @VisibleForTesting
  ImmutableRoaringBitmap getNullBitmap() {
    return _nullBitmap;
  }

  @Override
  public void close() {
  }
}
