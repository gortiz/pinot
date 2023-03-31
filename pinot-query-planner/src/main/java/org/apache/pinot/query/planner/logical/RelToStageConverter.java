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
package org.apache.pinot.query.planner.logical;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.query.planner.stage.WindowNode;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The {@code StageNodeConverter} converts a logical {@link RelNode} to a {@link StageNode}.
 */
public final class RelToStageConverter {

  private RelToStageConverter() {
    // do not instantiate.
  }

  /**
   * convert a normal relation node into stage node with just the expression piece.
   *
   * TODO: we should convert this to a more structured pattern once we determine the serialization format used.
   *
   * @param node relational node
   * @return stage node.
   */
  public static StageNode toStageNode(RelNode node, int currentStageId) {
    if (node instanceof LogicalTableScan) {
      return convertLogicalTableScan((LogicalTableScan) node, currentStageId);
    } else if (node instanceof LogicalJoin) {
      return convertLogicalJoin((LogicalJoin) node, currentStageId);
    } else if (node instanceof LogicalProject) {
      return convertLogicalProject((LogicalProject) node, currentStageId);
    } else if (node instanceof LogicalFilter) {
      return convertLogicalFilter((LogicalFilter) node, currentStageId);
    } else if (node instanceof LogicalAggregate) {
      return convertLogicalAggregate((LogicalAggregate) node, currentStageId);
    } else if (node instanceof LogicalSort) {
      return convertLogicalSort((LogicalSort) node, currentStageId);
    } else if (node instanceof LogicalValues) {
      return convertLogicalValues((LogicalValues) node, currentStageId);
    } else if (node instanceof LogicalWindow) {
      return convertLogicalWindow((LogicalWindow) node, currentStageId);
    } else {
      throw new UnsupportedOperationException("Unsupported logical plan node: " + node);
    }
  }

  private static StageNode convertLogicalValues(LogicalValues node, int currentStageId) {
    return new ValueNode(currentStageId, toDataSchema(node.getRowType()), node.tuples);
  }

  private static StageNode convertLogicalWindow(LogicalWindow node, int currentStageId) {
    return new WindowNode(currentStageId, node.groups, node.constants, toDataSchema(node.getRowType()));
  }

  private static StageNode convertLogicalSort(LogicalSort node, int currentStageId) {
    int fetch = RexExpressionUtils.getValueAsInt(node.fetch);
    int offset = RexExpressionUtils.getValueAsInt(node.offset);
    return new SortNode(currentStageId, node.getCollation().getFieldCollations(), fetch, offset,
        toDataSchema(node.getRowType()));
  }

  private static StageNode convertLogicalAggregate(LogicalAggregate node, int currentStageId) {
    return new AggregateNode(currentStageId, toDataSchema(node.getRowType()), node.getAggCallList(),
        RexExpression.toRexInputRefs(node.getGroupSet()), node.getHints());
  }

  private static StageNode convertLogicalProject(LogicalProject node, int currentStageId) {
    return new ProjectNode(currentStageId, toDataSchema(node.getRowType()), node.getProjects());
  }

  private static StageNode convertLogicalFilter(LogicalFilter node, int currentStageId) {
    return new FilterNode(currentStageId, toDataSchema(node.getRowType()), node.getCondition());
  }

  private static StageNode convertLogicalTableScan(LogicalTableScan node, int currentStageId) {
    String tableName = node.getTable().getQualifiedName().get(0);
    List<String> columnNames =
        node.getRowType().getFieldList().stream().map(RelDataTypeField::getName).collect(Collectors.toList());
    return new TableScanNode(currentStageId, toDataSchema(node.getRowType()), tableName, columnNames);
  }

  private static StageNode convertLogicalJoin(LogicalJoin node, int currentStageId) {
    JoinRelType joinType = node.getJoinType();

    // Parse out all equality JOIN conditions
    JoinInfo joinInfo = node.analyzeCondition();
    FieldSelectionKeySelector leftFieldSelectionKeySelector = new FieldSelectionKeySelector(joinInfo.leftKeys);
    FieldSelectionKeySelector rightFieldSelectionKeySelector = new FieldSelectionKeySelector(joinInfo.rightKeys);
    return new JoinNode(currentStageId, toDataSchema(node.getRowType()), toDataSchema(node.getLeft().getRowType()),
        toDataSchema(node.getRight().getRowType()), joinType,
        new JoinNode.JoinKeys(leftFieldSelectionKeySelector, rightFieldSelectionKeySelector),
        joinInfo.nonEquiConditions.stream().map(RexExpression::toRexExpression).collect(Collectors.toList()));
  }

  private static DataSchema toDataSchema(RelDataType rowType) {
    if (rowType instanceof RelRecordType) {
      RelRecordType recordType = (RelRecordType) rowType;
      String[] columnNames = recordType.getFieldNames().toArray(new String[]{});
      DataSchema.ColumnDataType[] columnDataTypes = new DataSchema.ColumnDataType[columnNames.length];
      for (int i = 0; i < columnNames.length; i++) {
        columnDataTypes[i] = convertToColumnDataType(recordType.getFieldList().get(i).getType());
      }
      return new DataSchema(columnNames, columnDataTypes);
    } else {
      throw new IllegalArgumentException("Unsupported RelDataType: " + rowType);
    }
  }

  public static DataSchema.ColumnDataType convertToColumnDataType(RelDataType relDataType) {
    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    boolean isArray = sqlTypeName == SqlTypeName.ARRAY;
    if (isArray) {
      sqlTypeName = relDataType.getComponentType().getSqlTypeName();
    }
    switch (sqlTypeName) {
      case BOOLEAN:
        return isArray ? DataSchema.ColumnDataType.BOOLEAN_ARRAY : DataSchema.ColumnDataType.BOOLEAN;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return isArray ? DataSchema.ColumnDataType.INT_ARRAY : DataSchema.ColumnDataType.INT;
      case BIGINT:
        return isArray ? DataSchema.ColumnDataType.LONG_ARRAY : DataSchema.ColumnDataType.LONG;
      case DECIMAL:
        return resolveDecimal(relDataType);
      case FLOAT:
        return isArray ? DataSchema.ColumnDataType.FLOAT_ARRAY : DataSchema.ColumnDataType.FLOAT;
      case REAL:
      case DOUBLE:
        return isArray ? DataSchema.ColumnDataType.DOUBLE_ARRAY : DataSchema.ColumnDataType.DOUBLE;
      case DATE:
      case TIME:
      case TIMESTAMP:
        return isArray ? DataSchema.ColumnDataType.TIMESTAMP_ARRAY : DataSchema.ColumnDataType.TIMESTAMP;
      case CHAR:
      case VARCHAR:
        return isArray ? DataSchema.ColumnDataType.STRING_ARRAY : DataSchema.ColumnDataType.STRING;
      case OTHER:
        return DataSchema.ColumnDataType.OBJECT;
      case BINARY:
      case VARBINARY:
        return isArray ? DataSchema.ColumnDataType.BYTES_ARRAY : DataSchema.ColumnDataType.BYTES;
      default:
        return DataSchema.ColumnDataType.BYTES;
    }
  }

  public static FieldSpec.DataType convertToFieldSpecDataType(RelDataType relDataType) {
    DataSchema.ColumnDataType columnDataType = convertToColumnDataType(relDataType);
    if (columnDataType == DataSchema.ColumnDataType.OBJECT) {
      return FieldSpec.DataType.BYTES;
    }
    return columnDataType.toDataType();
  }

  public static PinotDataType convertToPinotDataType(RelDataType relDataType) {
    return PinotDataType.getPinotDataTypeForExecution(convertToColumnDataType(relDataType));
  }

  /**
   * Calcite uses DEMICAL type to infer data type hoisting and infer arithmetic result types. down casting this
   * back to the proper primitive type for Pinot.
   *
   * @param relDataType the DECIMAL rel data type.
   * @return proper {@link DataSchema.ColumnDataType}.
   * @see {@link org.apache.calcite.rel.type.RelDataTypeFactoryImpl#decimalOf}.
   */
  private static DataSchema.ColumnDataType resolveDecimal(RelDataType relDataType) {
    int precision = relDataType.getPrecision();
    int scale = relDataType.getScale();
    if (scale == 0) {
      if (precision <= 10) {
        return DataSchema.ColumnDataType.INT;
      } else if (precision <= 38) {
        return DataSchema.ColumnDataType.LONG;
      } else {
        return DataSchema.ColumnDataType.BIG_DECIMAL;
      }
    } else {
      if (precision <= 14) {
        return DataSchema.ColumnDataType.FLOAT;
      } else if (precision <= 30) {
        return DataSchema.ColumnDataType.DOUBLE;
      } else {
        return DataSchema.ColumnDataType.BIG_DECIMAL;
      }
    }
  }
}
