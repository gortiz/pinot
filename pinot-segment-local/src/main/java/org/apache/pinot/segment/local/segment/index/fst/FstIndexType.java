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

package org.apache.pinot.segment.local.segment.index.fst;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.inv.text.LuceneFSTIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.FSTIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.LuceneFSTIndexReader;
import org.apache.pinot.segment.local.utils.nativefst.FSTHeader;
import org.apache.pinot.segment.local.utils.nativefst.NativeFSTIndexCreator;
import org.apache.pinot.segment.local.utils.nativefst.NativeFSTIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FstIndexConfig;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class FstIndexType implements IndexType<FstIndexConfig, TextIndexReader, TextIndexCreator>,
                                     ConfigurableFromIndexLoadingConfig<FstIndexConfig> {
  public static final FstIndexType INSTANCE = new FstIndexType();

  private FstIndexType() {
  }

  @Override
  public String getId() {
    return "fst";
  }

  @Override
  public String getIndexName() {
    return "fst_index";
  }

  @Override
  public Class<FstIndexConfig> getIndexConfigClass() {
    return FstIndexConfig.class;
  }

  @Override
  public Map<String, FstIndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    Map<String, FstIndexConfig> result = new HashMap<>();
    Set<String> fstIndexColumns = indexLoadingConfig.getFSTIndexColumns();
    for (String column : indexLoadingConfig.getAllKnownColumns()) {
      if (fstIndexColumns.contains(column)) {
        FstIndexConfig conf = new FstIndexConfig(indexLoadingConfig.getFSTIndexType());
        result.put(column, conf);
      } else {
        result.put(column, FstIndexConfig.DISABLED);
      }
    }
    return result;
  }

  @Override
  public FstIndexConfig getDefaultConfig() {
    return FstIndexConfig.DISABLED;
  }

  @Override
  public ColumnConfigDeserializer<FstIndexConfig> getDeserializer() {
    return IndexConfigDeserializer.fromIndexes(getId(), getIndexConfigClass())
        .withExclusiveAlternative(IndexConfigDeserializer.fromIndexTypes(
            FieldConfig.IndexType.FST,
            (tableConfig, fieldConfig) -> {
              IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
              FSTType fstIndexType = indexingConfig != null ? indexingConfig.getFSTIndexType() : null;
              return new FstIndexConfig(fstIndexType);
            }));
  }

  @Override
  public TextIndexCreator createIndexCreator(IndexCreationContext context, FstIndexConfig indexConfig)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().isSingleValueField(),
        "FST index is currently only supported on single-value columns");
    Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.STRING,
        "FST index is currently only supported on STRING type columns");
    Preconditions.checkState(context.hasDictionary(),
        "FST index is currently only supported on dictionary-encoded columns");
    if (indexConfig.getFstType() == FSTType.NATIVE) {
      return new NativeFSTIndexCreator(context);
    } else {
      return new LuceneFSTIndexCreator(context);
    }
  }

  @Override
  public ReaderFactory getReaderFactory() {
    return new ReaderFactory();
  }

  public static TextIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
      throws IndexReaderConstraintException, IOException {
    return INSTANCE.getReaderFactory().read(dataBuffer, metadata);
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new FSTIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.FST_INDEX_FILE_EXTENSION;
  }

  public static class ReaderFactory extends IndexReaderFactory.Default<FstIndexConfig, TextIndexReader> {
    @Override
    protected IndexType<FstIndexConfig, TextIndexReader, ?> getIndexType() {
      return FstIndexType.INSTANCE;
    }

    protected TextIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IndexReaderConstraintException, IOException {
      if (!metadata.hasDictionary()) {
        throw new IndexReaderConstraintException(metadata.getColumnName(), FstIndexType.INSTANCE,
            "This index requires a dictionary");
      }
      int magicHeader = dataBuffer.getInt(0);
      if (magicHeader == FSTHeader.FST_MAGIC) {
        return new NativeFSTIndexReader(dataBuffer);
      } else {
        return new LuceneFSTIndexReader(dataBuffer);
      }
    }

    @Override
    protected TextIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata, FstIndexConfig indexConfig)
        throws IndexReaderConstraintException, IOException {
      return read(dataBuffer, metadata);
    }
  }

  @Override
  public String toString() {
    return getId();
  }
}
