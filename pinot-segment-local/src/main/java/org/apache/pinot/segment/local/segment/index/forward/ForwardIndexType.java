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

package org.apache.pinot.segment.local.segment.index.forward;

import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;


public class ForwardIndexType implements IndexType<Object, IndexReader, IndexCreator> {

  public static final ForwardIndexType INSTANCE = new ForwardIndexType();

  private ForwardIndexType() {
  }

  @Override
  public String getId() {
    return "forward_index";
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    if (columnMetadata.isSingleValue()) {
      if (!columnMetadata.hasDictionary()) {
        return V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else if (columnMetadata.isSorted()) {
        return V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      } else {
        return V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;
      }
    } else if (!columnMetadata.hasDictionary()) {
      return V1Constants.Indexes.RAW_MV_FORWARD_INDEX_FILE_EXTENSION;
    } else {
      return V1Constants.Indexes.UNSORTED_MV_FORWARD_INDEX_FILE_EXTENSION;
    }
  }

  @Override
  public String toString() {
    return getId();
  }
}
