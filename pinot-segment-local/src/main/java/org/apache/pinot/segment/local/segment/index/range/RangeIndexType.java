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

package org.apache.pinot.segment.local.segment.index.range;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.RangeIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.BitSlicedRangeIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.RangeIndexReaderImpl;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexDeclaration;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


public class RangeIndexType implements IndexType<RangeIndexConfig, RangeIndexReader, CombinedInvertedIndexCreator>,
                                       ConfigurableFromIndexLoadingConfig<RangeIndexConfig> {

  /**
   * The default range index version used when not specified in the TableConfig.
   *
   * This value should be equal to the one used in {@link org.apache.pinot.spi.config.table.IndexingConfig}
   */
  public static final int DEFAULT_RANGE_INDEX_VERSION = 2;
  public static final RangeIndexType INSTANCE = new RangeIndexType();

  RangeIndexType() {
  }

  @Override
  public String getId() {
    return "range";
  }

  @Override
  public String getIndexName() {
    return "range_index";
  }

  @Override
  public Class<RangeIndexConfig> getIndexConfigClass() {
    return RangeIndexConfig.class;
  }

  @Nullable
  @Override
  public RangeIndexConfig deserialize(JsonNode node)
      throws IOException {
    if (node.isBoolean()) {
      if (node.booleanValue()) {
        return getDefaultConfig();
      } else {
        return null;
      }
    }
    if (node.isInt()) {
      return new RangeIndexConfig(node.asInt());
    }
    return JsonUtils.jsonNodeToObject(node, getIndexConfigClass());
  }

  @Override
  public Map<String, IndexDeclaration<RangeIndexConfig>> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    int rangeVersion = indexLoadingConfig.getRangeIndexVersion();
    return indexLoadingConfig.getRangeIndexColumns().stream()
        .collect(Collectors.toMap(
            Function.identity(),
            c -> IndexDeclaration.declared(new RangeIndexConfig(rangeVersion))));
  }

  @Override
  public IndexDeclaration<RangeIndexConfig> deserializeSpreadConf(TableConfig tableConfig, Schema schema,
      String column) {
    int rangeVersion = tableConfig.getIndexingConfig().getRangeIndexVersion();
    if (rangeVersion == 0) {
      rangeVersion = DEFAULT_RANGE_INDEX_VERSION;
    }
    List<String> rangeIndexColumns = tableConfig.getIndexingConfig().getRangeIndexColumns();
    if (rangeIndexColumns == null) {
      return IndexDeclaration.notDeclared(this);
    }
    if (!rangeIndexColumns.contains(column)) {
      return IndexDeclaration.declaredDisabled();
    }
    return IndexDeclaration.declared(new RangeIndexConfig(rangeVersion));
  }

  @Override
  public CombinedInvertedIndexCreator createIndexCreator(IndexCreationContext context, RangeIndexConfig indexConfig)
      throws IOException {
    if (indexConfig.getVersion() == BitSlicedRangeIndexCreator.VERSION && context.getFieldSpec().isSingleValueField()) {
      if (context.hasDictionary()) {
        return new BitSlicedRangeIndexCreator(context.getIndexDir(), context.getFieldSpec(), context.getCardinality());
      }
      return new BitSlicedRangeIndexCreator(context.getIndexDir(), context.getFieldSpec(), context.getMinValue(),
          context.getMaxValue());
    }
    // default to RangeIndexCreator for the time being
    return new RangeIndexCreator(context.getIndexDir(), context.getFieldSpec(),
        context.hasDictionary() ? FieldSpec.DataType.INT : context.getFieldSpec().getDataType(), -1,
        -1, context.getTotalDocs(), context.getTotalNumberOfEntries());
  }

  @Override
  public IndexReaderFactory<RangeIndexReader> getReaderFactory() {
    return new ReaderFactory();
  }

  public static RangeIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
      throws IndexReaderConstraintException {
    return ReaderFactory.read(dataBuffer, metadata);
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new RangeIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  @Override
  public String toString() {
    return getId();
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<RangeIndexConfig, RangeIndexReader> {
    @Override
    protected IndexType<RangeIndexConfig, RangeIndexReader, ?> getIndexType() {
      return RangeIndexType.INSTANCE;
    }

    @Override
    protected RangeIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata, RangeIndexConfig indexConfig,
        File segmentDir)
        throws IndexReaderConstraintException {
      return read(dataBuffer, metadata);
    }

    public static RangeIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IndexReaderConstraintException {
      int version = dataBuffer.getInt(0);
      if (version == RangeIndexCreator.VERSION) {
        return new RangeIndexReaderImpl(dataBuffer);
      } else if (version == BitSlicedRangeIndexCreator.VERSION) {
        return new BitSlicedRangeIndexReader(dataBuffer, metadata);
      }
      throw new IndexReaderConstraintException(metadata.getColumnName(), RangeIndexType.INSTANCE,
          "Unknown range index version " + version);
    }
  }
}
