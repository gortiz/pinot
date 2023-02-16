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
package org.apache.pinot.segment.spi.index.metadata;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.BytesUtils;


public class ColumnMetadataImpl implements ColumnMetadata {
  private final FieldSpec _fieldSpec;
  private final int _totalDocs;
  private final int _cardinality;
  private final boolean _sorted;
  private final Comparable<?> _minValue;
  private final Comparable<?> _maxValue;

  private final boolean _minMaxValueInvalid;
  private final boolean _hasDictionary;
  private final int _columnMaxLength;
  private final int _bitsPerElement;
  private final int _maxNumberOfMultiValues;
  private final int _totalNumberOfEntries;
  private final PartitionFunction _partitionFunction;
  private final Set<Integer> _partitions;
  private final Map<ColumnIndexType, Long> _indexSizeMap;
  private final boolean _autoGenerated;

  private ColumnMetadataImpl(FieldSpec fieldSpec, int totalDocs, int cardinality, boolean sorted,
      Comparable<?> minValue, Comparable<?> maxValue, boolean minMaxValueInvalid, boolean hasDictionary,
      int columnMaxLength, int bitsPerElement, int maxNumberOfMultiValues, int totalNumberOfEntries,
      @Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions,
      Map<ColumnIndexType, Long> indexSizeMap, boolean autoGenerated) {
    _fieldSpec = fieldSpec;
    _totalDocs = totalDocs;
    _cardinality = cardinality;
    _sorted = sorted;
    _minValue = minValue;
    _maxValue = maxValue;
    _minMaxValueInvalid = minMaxValueInvalid;
    _hasDictionary = hasDictionary;
    _columnMaxLength = columnMaxLength;
    _bitsPerElement = bitsPerElement;
    _maxNumberOfMultiValues = maxNumberOfMultiValues;
    _totalNumberOfEntries = totalNumberOfEntries;
    _partitionFunction = partitionFunction;
    _partitions = partitions;
    _indexSizeMap = indexSizeMap;
    _autoGenerated = autoGenerated;
  }

  @Override
  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  @Override
  public int getTotalDocs() {
    return _totalDocs;
  }

  @Override
  public int getCardinality() {
    return _cardinality;
  }

  @Override
  public boolean isSorted() {
    return _sorted;
  }

  @Override
  public Comparable<?> getMinValue() {
    return _minValue;
  }

  @Override
  public Comparable<?> getMaxValue() {
    return _maxValue;
  }

  public boolean isMinMaxValueInvalid() {
    return _minMaxValueInvalid;
  }

  @Override
  public boolean hasDictionary() {
    return _hasDictionary;
  }

  @Override
  public int getColumnMaxLength() {
    return _columnMaxLength;
  }

  @Override
  public int getBitsPerElement() {
    return _bitsPerElement;
  }

  @Override
  public int getMaxNumberOfMultiValues() {
    return _maxNumberOfMultiValues;
  }

  @Override
  public int getTotalNumberOfEntries() {
    return _totalNumberOfEntries;
  }

  @Nullable
  @Override
  public PartitionFunction getPartitionFunction() {
    return _partitionFunction;
  }

  @Nullable
  @Override
  public Set<Integer> getPartitions() {
    return _partitions;
  }

  @Nullable
  @Override
  public Map<ColumnIndexType, Long> getIndexSizeMap() {
    return _indexSizeMap;
  }

  @Override
  public boolean isAutoGenerated() {
    return _autoGenerated;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnMetadataImpl that = (ColumnMetadataImpl) o;
    return _totalDocs == that._totalDocs && _cardinality == that._cardinality && _sorted == that._sorted
        && _hasDictionary == that._hasDictionary && _columnMaxLength == that._columnMaxLength
        && _bitsPerElement == that._bitsPerElement && _maxNumberOfMultiValues == that._maxNumberOfMultiValues
        && _totalNumberOfEntries == that._totalNumberOfEntries && _autoGenerated == that._autoGenerated
        && Objects.equals(_fieldSpec, that._fieldSpec) && Objects.equals(_minValue, that._minValue) && Objects.equals(
        _maxValue, that._maxValue) && Objects.equals(_partitionFunction, that._partitionFunction) && Objects.equals(
        _partitions, that._partitions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_fieldSpec, _totalDocs, _cardinality, _sorted, _minValue, _maxValue, _hasDictionary,
        _columnMaxLength, _bitsPerElement, _maxNumberOfMultiValues, _totalNumberOfEntries, _partitionFunction,
        _partitions, _autoGenerated);
  }

  @Override
  public String toString() {
    return "ColumnMetadataImpl{" + "_fieldSpec=" + _fieldSpec + ", _totalDocs=" + _totalDocs + ", _cardinality="
        + _cardinality + ", _sorted=" + _sorted + ", _minValue=" + _minValue + ", _maxValue=" + _maxValue
        + ", _hasDictionary=" + _hasDictionary + ", _columnMaxLength=" + _columnMaxLength + ", _bitsPerElement="
        + _bitsPerElement + ", _maxNumberOfMultiValues=" + _maxNumberOfMultiValues + ", _totalNumberOfEntries="
        + _totalNumberOfEntries + ", _partitionFunction=" + _partitionFunction + ", _partitions=" + _partitions
        + ", _autoGenerated=" + _autoGenerated + '}';
  }

  public static ColumnMetadataImpl fromPropertiesConfiguration(String column, PropertiesConfiguration config) {
    Builder builder = new Builder().setTotalDocs(config.getInt(Column.getKeyFor(column, Column.TOTAL_DOCS)))
        .setCardinality(config.getInt(Column.getKeyFor(column, Column.CARDINALITY)))
        .setSorted(config.getBoolean(Column.getKeyFor(column, Column.IS_SORTED), false))
        .setHasDictionary(config.getBoolean(Column.getKeyFor(column, Column.HAS_DICTIONARY), true))
        .setBitsPerElement(config.getInt(Column.getKeyFor(column, Column.BITS_PER_ELEMENT)))
        .setColumnMaxLength(config.getInt(Column.getKeyFor(column, Column.DICTIONARY_ELEMENT_SIZE)))
        .setMaxNumberOfMultiValues(config.getInt(Column.getKeyFor(column, Column.MAX_MULTI_VALUE_ELEMENTS)))
        .setTotalNumberOfEntries(config.getInt(Column.getKeyFor(column, Column.TOTAL_NUMBER_OF_ENTRIES)))
        .setAutoGenerated(config.getBoolean(Column.getKeyFor(column, Column.IS_AUTO_GENERATED), false));

    FieldType fieldType =
        FieldType.valueOf(config.getString(Column.getKeyFor(column, Column.COLUMN_TYPE)).toUpperCase());
    DataType dataType = DataType.valueOf(config.getString(Column.getKeyFor(column, Column.DATA_TYPE)).toUpperCase());
    String defaultNullValueString = config.getString(Column.getKeyFor(column, Column.DEFAULT_NULL_VALUE), null);
    FieldSpec fieldSpec;
    switch (fieldType) {
      case DIMENSION:
        boolean isSingleValue = config.getBoolean(Column.getKeyFor(column, Column.IS_SINGLE_VALUED));
        fieldSpec = new DimensionFieldSpec(column, dataType, isSingleValue, defaultNullValueString);
        break;
      case METRIC:
        fieldSpec = new MetricFieldSpec(column, dataType, defaultNullValueString);
        break;
      case TIME:
        TimeUnit timeUnit = TimeUnit.valueOf(config.getString(Segment.TIME_UNIT, "DAYS").toUpperCase());
        fieldSpec = new TimeFieldSpec(new TimeGranularitySpec(dataType, timeUnit, column));
        break;
      case DATE_TIME:
        String format = config.getString(Column.getKeyFor(column, Column.DATETIME_FORMAT));
        String granularity = config.getString(Column.getKeyFor(column, Column.DATETIME_GRANULARITY));
        fieldSpec = new DateTimeFieldSpec(column, dataType, format, granularity, defaultNullValueString, null);
        break;
      default:
        throw new IllegalStateException("Unsupported field type: " + fieldType);
    }
    builder.setFieldSpec(fieldSpec);

    // Set min/max value if available
    // NOTE: Use getProperty() instead of getString() to avoid variable substitution ('${anotherKey}'), which can cause
    //       problem for special values such as '$${' where the first '$' is identified as escape character.
    // TODO: Use getProperty() for other properties as well to avoid the overhead of variable substitution
    String minString = (String) config.getProperty(Column.getKeyFor(column, Column.MIN_VALUE));
    String maxString = (String) config.getProperty(Column.getKeyFor(column, Column.MAX_VALUE));
    if (minString != null && maxString != null) {
      switch (dataType.getStoredType()) {
        case INT:
          builder.setMinValue(Integer.valueOf(minString));
          builder.setMaxValue(Integer.valueOf(maxString));
          break;
        case LONG:
          builder.setMinValue(Long.valueOf(minString));
          builder.setMaxValue(Long.valueOf(maxString));
          break;
        case FLOAT:
          builder.setMinValue(Float.valueOf(minString));
          builder.setMaxValue(Float.valueOf(maxString));
          break;
        case DOUBLE:
          builder.setMinValue(Double.valueOf(minString));
          builder.setMaxValue(Double.valueOf(maxString));
          break;
        case BIG_DECIMAL:
          builder.setMinValue(new BigDecimal(minString));
          builder.setMaxValue(new BigDecimal(maxString));
          break;
        case STRING:
          builder.setMinValue(minString);
          builder.setMaxValue(maxString);
          break;
        case BYTES:
          builder.setMinValue(BytesUtils.toByteArray(minString));
          builder.setMaxValue(BytesUtils.toByteArray(maxString));
          break;
        default:
          throw new IllegalStateException("Unsupported data type: " + dataType + " for column: " + column);
      }
    }
    builder.setMinMaxValueInvalid(config.getBoolean(Column.getKeyFor(column, Column.MIN_MAX_VALUE_INVALID), false));

    // Only support zero padding
    String padding = config.getString(Segment.SEGMENT_PADDING_CHARACTER, null);
    Preconditions.checkState(String.valueOf(V1Constants.Str.DEFAULT_STRING_PAD_CHAR)
        .equals(StringEscapeUtils.unescapeJava(padding)), "Got non-zero string padding: %s", padding);

    String partitionFunctionName = config.getString(Column.getKeyFor(column, Column.PARTITION_FUNCTION), null);
    if (partitionFunctionName != null) {
      int numPartitions = config.getInt(Column.getKeyFor(column, Column.NUM_PARTITIONS));
      Map<String, String> partitionFunctionConfigMap = null;
      Configuration partitionFunctionConfig = config.subset(Column.getKeyFor(column, Column.PARTITION_FUNCTION_CONFIG));
      if (!partitionFunctionConfig.isEmpty()) {
        partitionFunctionConfigMap = new HashMap<>();
        Iterator<String> partitionFunctionConfigKeysIter = partitionFunctionConfig.getKeys();
        while (partitionFunctionConfigKeysIter.hasNext()) {
          String functionConfigKey = partitionFunctionConfigKeysIter.next();
          Object functionConfigValueObj = partitionFunctionConfig.getProperty(functionConfigKey);
          /*
          A partition function config value can have comma and this value is read as a List from
          PropertiesConfiguration.getProperty, Hence we need to rebuild original comma separated string value from
          this list of values.
           */
          partitionFunctionConfigMap.put(functionConfigKey,
              functionConfigValueObj instanceof List ? String.join(",", (List) functionConfigValueObj)
                  : functionConfigValueObj.toString());
        }
      }
      PartitionFunction partitionFunction =
          PartitionFunctionFactory.getPartitionFunction(partitionFunctionName, numPartitions,
              partitionFunctionConfigMap);
      builder.setPartitionFunction(partitionFunction);
      builder.setPartitions(
          ColumnPartitionMetadata.extractPartitions(config.getList(Column.getKeyFor(column, Column.PARTITION_VALUES))));
    }

    return builder.build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private FieldSpec _fieldSpec;
    private int _totalDocs;
    private int _cardinality;
    private boolean _sorted;
    private Comparable<?> _minValue;
    private Comparable<?> _maxValue;
    private boolean _minMaxValueInvalid;
    private boolean _hasDictionary;
    private int _columnMaxLength;
    private int _bitsPerElement;
    private int _maxNumberOfMultiValues;
    private int _totalNumberOfEntries;
    private PartitionFunction _partitionFunction;
    private Set<Integer> _partitions;
    private boolean _autoGenerated;
    private Map<ColumnIndexType, Long> _indexSizeMap = new HashMap<>();

    public Builder setFieldSpec(FieldSpec fieldSpec) {
      _fieldSpec = fieldSpec;
      return this;
    }

    public Builder setTotalDocs(int totalDocs) {
      _totalDocs = totalDocs;
      return this;
    }

    public Builder setCardinality(int cardinality) {
      _cardinality = cardinality;
      return this;
    }

    public Builder setSorted(boolean sorted) {
      _sorted = sorted;
      return this;
    }

    public Builder setMinValue(Comparable<?> minValue) {
      _minValue = minValue;
      return this;
    }

    public Builder setMaxValue(Comparable<?> maxValue) {
      _maxValue = maxValue;
      return this;
    }

    public Builder setMinMaxValueInvalid(boolean minMaxValueInvalid) {
      _minMaxValueInvalid = minMaxValueInvalid;
      return this;
    }

    public Builder setHasDictionary(boolean hasDictionary) {
      _hasDictionary = hasDictionary;
      return this;
    }

    public Builder setColumnMaxLength(int columnMaxLength) {
      _columnMaxLength = columnMaxLength;
      return this;
    }

    public Builder setBitsPerElement(int bitsPerElement) {
      _bitsPerElement = bitsPerElement;
      return this;
    }

    public Builder setMaxNumberOfMultiValues(int maxNumberOfMultiValues) {
      _maxNumberOfMultiValues = maxNumberOfMultiValues;
      return this;
    }

    public Builder setTotalNumberOfEntries(int totalNumberOfEntries) {
      _totalNumberOfEntries = totalNumberOfEntries;
      return this;
    }

    public Builder setPartitionFunction(PartitionFunction partitionFunction) {
      _partitionFunction = partitionFunction;
      return this;
    }

    public Builder setPartitions(Set<Integer> partitions) {
      _partitions = partitions;
      return this;
    }

    public void setIndexSizeMap(Map<ColumnIndexType, Long> indexSizeMap) {
      _indexSizeMap = indexSizeMap;
    }

    public Builder setAutoGenerated(boolean autoGenerated) {
      _autoGenerated = autoGenerated;
      return this;
    }

    public ColumnMetadataImpl build() {
      return new ColumnMetadataImpl(_fieldSpec, _totalDocs, _cardinality, _sorted, _minValue, _maxValue,
          _minMaxValueInvalid, _hasDictionary, _columnMaxLength, _bitsPerElement, _maxNumberOfMultiValues,
          _totalNumberOfEntries, _partitionFunction, _partitions, _indexSizeMap, _autoGenerated);
    }
  }
}
