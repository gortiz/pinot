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

package org.apache.pinot.segment.spi.index;

import java.io.File;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;


public interface IndexReaderFactory<R extends IndexReader> {

  /**
   * @throws IndexReaderConstraintException if the constraints of the index reader are not matched. For example, some
   * indexes may require the column to be dictionary based.
   */
  @Nullable
  R read(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs, ColumnMetadata metadata,
      File segmentDir)
      throws IOException, IndexReaderConstraintException;

  abstract class Default<C, R extends IndexReader> implements IndexReaderFactory<R> {

    protected abstract IndexType<C, R, ?> getIndexType();

    protected abstract R read(PinotDataBuffer dataBuffer, ColumnMetadata metadata, C indexConfig, File segmentDir)
        throws IOException, IndexReaderConstraintException;

    @Override
    public R read(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs,
        ColumnMetadata metadata, File segmentDir)
        throws IOException, IndexReaderConstraintException {
      IndexType<C, R, ?> indexType = getIndexType();
      IndexDeclaration<C> declaration;
      if (fieldIndexConfigs == null) {
        declaration = IndexDeclaration.notDeclared(indexType);
      } else {
        declaration = fieldIndexConfigs.getConfig(indexType);
      }

      C indexConf = declaration.getConfigOrNull();
      if (indexConf == null) { //it is either not enabled or the default value is null
        return null;
      }

      PinotDataBuffer buffer = segmentReader.getIndexFor(metadata.getColumnName(), indexType);
      try {
        return read(buffer, metadata, indexConf, segmentDir);
      } catch (RuntimeException ex) {
        throw new RuntimeException(
            "Cannot read index " + indexType + " for column " + metadata.getColumnName(), ex);
      }
    }
  }
}
