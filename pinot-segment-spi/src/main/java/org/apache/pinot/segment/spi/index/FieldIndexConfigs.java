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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig;


/**
 * FieldIndexConfigs are a map like structure that relates index types with their configuration, providing a type safe
 * interface.
 *
 * This class can be serialized into a JSON object whose keys are the index type ids using Jackson, but to be
 * deserialized it is necessary to
 * call {@link FieldIndexConfigs#deserializeJsonNode(JsonNode, boolean)}. A custom Jackson deserializer could be
 * provided if needed.
 */
public class FieldIndexConfigs {

  private final Map<IndexType, IndexDeclaration<?>> _configMap;
  public static final FieldIndexConfigs EMPTY = new FieldIndexConfigs(new HashMap<>());

  private FieldIndexConfigs(Map<IndexType, IndexDeclaration<?>> configMap) {
    _configMap = Collections.unmodifiableMap(configMap);
  }


  /**
   * The method most of the code should call to know how is configured a given index.
   *
   * @param indexType the type of the index we are interested in.
   * @return How the index has been configured, which can be {@link IndexDeclaration#isDeclared() not declared},
   * {@link IndexDeclaration#isEnabled()} not enabled} or an actual configuration object (which can be obtained with
   * {@link IndexDeclaration#getEnabledConfig()}.
   */
  @JsonIgnore
  public <C, I extends IndexType<C, ?, ?>> IndexDeclaration<C> getConfig(I indexType) {
    @SuppressWarnings("unchecked")
    IndexDeclaration<C> config = (IndexDeclaration<C>) _configMap.get(indexType);
    if (config == null) {
      return IndexDeclaration.notDeclared(indexType);
    }
    return config;
  }

  /*
  This is used by Jackson when this object is serialized. Each entry of the map will be directly contained in the
  JSON object, with the key name as the key in the JSON object and the result of serializing the key value as the value
  in the JSON object.
   */
  @JsonAnyGetter
  public Map<String, Object> unwrapIndexes() {
    Function<Map.Entry<IndexType, IndexDeclaration<?>>, JsonNode> serializer =
        entry -> entry.getKey().serialize(entry.getValue().getEnabledConfig());
    return _configMap.entrySet().stream()
        .filter(e -> e.getValue().isDeclared())
        .collect(Collectors.toMap(entry -> entry.getKey().getId(), serializer));
  }

  public static FieldIndexConfigs deserializeJsonNode(JsonNode node, boolean failIfUnrecognized)
      throws UnrecognizedIndexException, IOException {
    Iterator<Map.Entry<String, JsonNode>> it = node.fields();

    FieldIndexConfigs.Builder builder = new Builder();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      String indexId = entry.getKey();

      Optional<IndexType<?, ?, ?>> opt = IndexService.getInstance().getIndexTypeById(indexId);
      if (!opt.isPresent()) {
        if (failIfUnrecognized) {
          throw new UnrecognizedIndexException(indexId);
        } else {
          continue;
        }
      }
      IndexType<?, ?, ?> indexType = opt.get();
      JsonNode indexNode = entry.getValue();
      Object deserialized = indexType.deserialize(indexNode);
      builder.addUnsafe(indexType, deserialized);
    }
    return builder.build();
  }

  public static class Builder {
    private final Map<IndexType, IndexDeclaration<?>> _configMap;

    public Builder() {
      _configMap = new HashMap<>();
    }

    public Builder(FieldIndexConfigs other) {
      _configMap = new HashMap<>(other._configMap);
    }

    public <C, I extends IndexType<C, ?, ?>> Builder add(I indexType, @Nullable C config) {
      _configMap.put(indexType, IndexDeclaration.declared(config));
      return this;
    }

    public <C, I extends IndexType<C, ?, ?>> Builder addDeclaration(I indexType, IndexDeclaration<C> declaration) {
      if (!declaration.isDeclared()) {
        undeclare(indexType);
      } else {
        _configMap.put(indexType, declaration);
      }
      return this;
    }

    public Builder addUnsafe(IndexType<?, ?, ?> indexType, @Nullable Object config) {
      Preconditions.checkArgument(!(config instanceof IndexDeclaration), "Index declarations cannot be "
          + "added as values");
      _configMap.put(indexType, IndexDeclaration.declared(config));
      return this;
    }

    public Builder addUnsafeDeclaration(IndexType<?, ?, ?> indexType, @Nullable IndexDeclaration<?> config) {
      if (!config.isDeclared()) {
        undeclare(indexType);
      } else {
        _configMap.put(indexType, config);
      }
      return this;
    }

    public Builder undeclare(IndexType<?, ?, ?> indexType) {
      _configMap.remove(indexType);
      return this;
    }

    public FieldIndexConfigs build() {
      return new FieldIndexConfigs(_configMap);
    }
  }

  public static Map<String, FieldIndexConfigs> readFieldIndexConfigByColumn(Collection<FieldConfig> fieldConfigs,
      boolean failIfUnrecognized, Set<String> allColumns) {
    Map<String, FieldIndexConfigs> indexConfigsByColName = Maps.newHashMapWithExpectedSize(fieldConfigs.size());
    for (FieldConfig fieldConfig : fieldConfigs) {
      JsonNode indexes = fieldConfig.getIndexes();
      FieldIndexConfigs configs;
      try {
        configs = FieldIndexConfigs.deserializeJsonNode(indexes, failIfUnrecognized);
      } catch (IOException e) {
        throw new UncheckedIOException("Error deserializing index config of " + fieldConfig.getName() + " field", e);
      }
      indexConfigsByColName.put(fieldConfig.getName(), configs);
    }
    for (String column : allColumns) {
      if (!indexConfigsByColName.containsKey(column)) {
        indexConfigsByColName.put(column, FieldIndexConfigs.EMPTY);
      }
    }
    return indexConfigsByColName;
  }

  public static class UnrecognizedIndexException extends RuntimeException {
    private final String _indexId;

    public UnrecognizedIndexException(String indexId) {
      super("There is no index type whose identified as " + indexId);
      _indexId = indexId;
    }

    public String getIndexId() {
      return _indexId;
    }
  }
}
