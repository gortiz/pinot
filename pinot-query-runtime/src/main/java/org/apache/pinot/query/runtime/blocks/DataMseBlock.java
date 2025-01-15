package org.apache.pinot.query.runtime.blocks;

import java.util.List;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Block;


public interface DataMseBlock extends Block {

  List<Object[]> getRows();

  DataSchema getDataSchema();

  public static DataMseBlock fromDataBlock(DataBlock block) {

  }

  public static DataMseBlock fromTransferableBlock(TransferableBlock block) {

  }

  public static DataMseBlock fromRows(List<Object[]> dataBlock, DataSchema schema) {

  }
}
