package org.apache.pinot.segment.local.realtime.impl.forward;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.segment.local.io.reader.impl.FixedByteSingleValueMultiColReader;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteSingleValueMultiColWriter;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FixedByteSVMultiColForwardIndex implements MutableForwardIndex {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedByteSVMultiColForwardIndex.class);

  private final List<FixedByteSVMultiColForwardIndex.WriterWithOffset> _writers = new ArrayList<>();
  private final List<FixedByteSVMultiColForwardIndex.ReaderWithOffset> _readers = new ArrayList<>();

  private final boolean _dictionaryEncoded;
  private final FieldSpec.DataType _valueType;
  private final int _valueSizeInBytes;
  private final int _numRowsPerChunk;
  private final long _chunkSizeInBytes;

  private final PinotDataBufferMemoryManager _memoryManager;
  private final String _allocationContext;
  private int _capacityInRows = 0;
  private int _numColumns;
  private int[] _columnDataSize;

  /**
   * @param valueType Data type of the values
   * @param numRowsPerChunk Number of rows to pack in one chunk before a new chunk is created.
   * @param memoryManager Memory manager to be used for allocating memory.
   * @param allocationContext Allocation allocationContext.
   */
  public FixedByteSVMultiColForwardIndex(boolean dictionaryEncoded, FieldSpec.DataType valueType, int numRowsPerChunk,
      PinotDataBufferMemoryManager memoryManager, String allocationContext) {
    _dictionaryEncoded = dictionaryEncoded;
    _valueType = valueType;
    _valueSizeInBytes = valueType.size();
    _numRowsPerChunk = numRowsPerChunk;
    _chunkSizeInBytes = numRowsPerChunk * _valueSizeInBytes;
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    addBuffer();
  }

  public FixedByteSVMultiColForwardIndex(boolean dictionaryEncoded, int numRowsPerChunk,
      PinotDataBufferMemoryManager memoryManager, String allocationContext, FieldSpec.DataType[] columnDataTypes) {
    _dictionaryEncoded = dictionaryEncoded;
    _valueType = FieldSpec.DataType.BYTES;
    _numColumns = columnDataTypes.length;
    _valueSizeInBytes = Arrays.stream(columnDataTypes).map(FieldSpec.DataType::size).reduce(0, Integer::sum);
    _columnDataSize = Arrays.stream(columnDataTypes).mapToInt(FieldSpec.DataType::size).toArray();
    _numRowsPerChunk = numRowsPerChunk;
    _chunkSizeInBytes = numRowsPerChunk * _valueSizeInBytes;
    _memoryManager = memoryManager;
    _allocationContext = allocationContext;
    addBuffer();
  }

  @Override
  public boolean isDictionaryEncoded() {
    return _dictionaryEncoded;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public FieldSpec.DataType getValueType() {
    return _valueType;
  }

  @Override
  public int getLengthOfShortestElement() {
    return _valueSizeInBytes;
  }

  @Override
  public int getLengthOfLongestElement() {
    return _valueSizeInBytes;
  }

  @Override
  public int getDictId(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getInt(docId);
  }

  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer) {
    /*
     * TODO
     * If we assume that the document ids are sorted, then we can write logic to move values from one reader
     * at a time, identifying the rows in sequence that belong to the same block. This logic is more complex, but may
     * perform better in the sorted case.
     *
     * An alternative is to not have multiple _dataBuffers, but just copy the values from one buffer to next as we
     * increase the number of rows.
     */
    if (_readers.size() == 1) {
      _readers.get(0).getReader().readIntValues(docIds, 0, 0, length, dictIdBuffer, 0);
    } else {
      for (int i = 0; i < length; i++) {
        int docId = docIds[i];
        dictIdBuffer[i] = _readers.get(getBufferId(docId)).getInt(docId);
      }
    }
  }

  @Override
  public int getInt(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getInt(docId);
  }

  @Override
  public long getLong(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getLong(docId);
  }

  public int getInt(int docId, int col) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getInt(docId, col);
  }

  public long getLong(int docId, int col) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getLong(docId, col);
  }

  @Override
  public float getFloat(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getFloat(docId);
  }

  @Override
  public double getDouble(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getDouble(docId);
  }

  @Override
  public BigDecimal getBigDecimal(int docId) {
    int bufferId = getBufferId(docId);
    return _readers.get(bufferId).getBigDecimal(docId);
  }

  private int getBufferId(int row) {
    return row / _numRowsPerChunk;
  }

  @Override
  public void setDictId(int docId, int dictId) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setInt(docId, dictId);
  }

  @Override
  public void setInt(int docId, int value) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setInt(docId, value);
  }

  public void setInt(int docId, int col, int value) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setInt(docId, col, value);
  }

  public void setLong(int docId, int col, long value) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setLong(docId, col, value);
  }

  @Override
  public void setFloat(int docId, float value) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setFloat(docId, value);
  }

  @Override
  public void setDouble(int docId, double value) {
    addBufferIfNeeded(docId);
    getWriterForRow(docId).setDouble(docId, value);
  }

  private FixedByteSVMultiColForwardIndex.WriterWithOffset getWriterForRow(int row) {
    return _writers.get(getBufferId(row));
  }

  @Override
  public void close()
      throws IOException {
    for (FixedByteSVMultiColForwardIndex.WriterWithOffset writer : _writers) {
      writer.close();
    }
    for (FixedByteSVMultiColForwardIndex.ReaderWithOffset reader : _readers) {
      reader.close();
    }
  }

  private void addBuffer() {
    LOGGER.info("Allocating {} bytes for: {}", _chunkSizeInBytes, _allocationContext);
    // NOTE: PinotDataBuffer is tracked in the PinotDataBufferMemoryManager. No need to track it inside the class.
    PinotDataBuffer buffer = _memoryManager.allocate(_chunkSizeInBytes, _allocationContext);
    _writers.add(
        new FixedByteSVMultiColForwardIndex.WriterWithOffset(new FixedByteSingleValueMultiColWriter(buffer, _numColumns, _columnDataSize),
            _capacityInRows));
    _readers.add(new FixedByteSVMultiColForwardIndex.ReaderWithOffset(
        new FixedByteSingleValueMultiColReader(buffer, _numRowsPerChunk, _columnDataSize),
        _capacityInRows));
    _capacityInRows += _numRowsPerChunk;
  }

  /**
   * Helper class that encapsulates writer and global startRowId.
   */
  private void addBufferIfNeeded(int row) {
    if (row >= _capacityInRows) {

      // Adding _chunkSizeInBytes in the numerator for rounding up. +1 because rows are 0-based index.
      long buffersNeeded = (row + 1 - _capacityInRows + _numRowsPerChunk) / _numRowsPerChunk;
      for (int i = 0; i < buffersNeeded; i++) {
        addBuffer();
      }
    }
  }

  private static class WriterWithOffset implements Closeable {
    final FixedByteSingleValueMultiColWriter _writer;
    final int _startRowId;

    private WriterWithOffset(FixedByteSingleValueMultiColWriter writer, int startRowId) {
      _writer = writer;
      _startRowId = startRowId;
    }

    @Override
    public void close()
        throws IOException {
      _writer.close();
    }

    public void setInt(int row, int value) {
      _writer.setInt(row - _startRowId, 0, value);
    }

    public void setLong(int row, long value) {
      _writer.setLong(row - _startRowId, 0, value);
    }

    public void setInt(int row, int col, int value) {
      _writer.setInt(row - _startRowId, col, value);
    }

    public void setLong(int row, int col, long value) {
      _writer.setLong(row - _startRowId, col, value);
    }

    public void setFloat(int row, float value) {
      _writer.setFloat(row - _startRowId, 0, value);
    }

    public void setDouble(int row, double value) {
      _writer.setDouble(row - _startRowId, 0, value);
    }
  }

  /**
   * Helper class that encapsulates reader and global startRowId.
   */
  private static class ReaderWithOffset implements Closeable {
    final FixedByteSingleValueMultiColReader _reader;
    final int _startRowId;

    private ReaderWithOffset(FixedByteSingleValueMultiColReader reader, int startRowId) {
      _reader = reader;
      _startRowId = startRowId;
    }

    @Override
    public void close()
        throws IOException {
      _reader.close();
    }

    public int getInt(int row) {
      return _reader.getInt(row - _startRowId, 0);
    }

    public long getLong(int row) {
      return _reader.getLong(row - _startRowId, 0);
    }
    public int getInt(int row, int col) {
      return _reader.getInt(row - _startRowId, col);
    }

    public long getLong(int row, int col) {
      return _reader.getLong(row - _startRowId, col);
    }

    public float getFloat(int row) {
      return _reader.getFloat(row - _startRowId, 0);
    }

    public double getDouble(int row) {
      return _reader.getDouble(row - _startRowId, 0);
    }

    public BigDecimal getBigDecimal(int row) {
      return BigDecimalUtils.deserialize(_reader.getBytes(row - _startRowId, 0));
    }

    public FixedByteSingleValueMultiColReader getReader() {
      return _reader;
    }
  }
}
