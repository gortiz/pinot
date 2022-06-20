package org.apache.pinot.segment.local.upsert;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.PeekableIntIterator;


public class UpsertRunnerRocksDB {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "UpsertRunner");
  private static final String TABLE_NAME = "MyTable";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String LONG_COL_NAME = "LONG_COL";
  private static final String SORTED_COL_NAME = "SORTED_COL";
  private static final String RAW_INT_COL_NAME = "RAW_INT_COL";
  private static final String RAW_STRING_COL_NAME = "RAW_STRING_COL";
  private static final String NO_INDEX_INT_COL_NAME = "NO_INDEX_INT_COL";
  private static final String NO_INDEX_STRING_COL = "NO_INDEX_STRING_COL";
  private static final String LOW_CARDINALITY_STRING_COL = "LOW_CARDINALITY_STRING_COL";

  private static LongSupplier _supplier;
  static String _scenario = "EXP(0.5)";
  static int _numRows = 10;
  static int _numSegments = 10;
  private static  List<IndexSegment> _indexSegments;
  private static String[] lowCardinalityValues = IntStream.range(0, 10).mapToObj(i -> "value" + i)
      .toArray(String[]::new);

  public static void main(String[] args) throws Exception {
    _supplier = Distribution.createLongSupplier(42, _scenario);
    _indexSegments = new ArrayList<>();

    FileUtils.deleteQuietly(INDEX_DIR);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    PartitionUpsertRocksDBMetadataManager partitionUpsertOffHeapMetadataManager = new PartitionUpsertRocksDBMetadataManager(TABLE_NAME, 0, null, null, HashFunction.NONE);

    for (int i = 0; i < _numSegments; i++) {
      String name = "segment_" + i;
      buildSegment(name);
      ImmutableSegmentImpl immutableSegment =
          handleUpsert(indexLoadingConfig, partitionUpsertOffHeapMetadataManager,
              name);
      _indexSegments.add(immutableSegment);
    }

    for (int i = 0; i < _numSegments; i++) {
      ImmutableSegmentImpl immutableSegment = (ImmutableSegmentImpl) _indexSegments.get(i);
      String name = immutableSegment.getSegmentName();
      PeekableIntIterator intIterator = immutableSegment.getValidDocIds().getMutableRoaringBitmap().getIntIterator();
      System.out.println("SEGMENT NAME: " + name);
      while (intIterator.hasNext()) {
        int docId = intIterator.next();
        System.out.println(docId+ ": "
            + immutableSegment.getRecord(docId, new GenericRow()).getValue(LOW_CARDINALITY_STRING_COL)
        + ": " + immutableSegment.getRecord(docId, new GenericRow()).getValue(LONG_COL_NAME));
      }
    }

    System.out.println("NUM SEGMENTS CREATED: " + _indexSegments.size());

//    for(IndexSegment indexSegment: _indexSegments) {
//      for(int i=0; i<10; i++) {
//        Object[] pk = new Object[]{lowCardinalityValues[i % lowCardinalityValues.length]};
//        partitionUpsertOffHeapMetadataManager.addRecord(indexSegment, new RecordInfo(new PrimaryKey(pk), i, _supplier.getAsLong()));
//      }
//    }
    close();
  }

  private static ImmutableSegmentImpl handleUpsert(IndexLoadingConfig indexLoadingConfig,
      PartitionUpsertRocksDBMetadataManager partitionUpsertOffHeapMetadataManager, String name)
      throws Exception {
    ImmutableSegmentImpl immutableSegment = (ImmutableSegmentImpl) ImmutableSegmentLoader.load(new File(INDEX_DIR, name),
        indexLoadingConfig);
    immutableSegment.enableUpsert(null, new ThreadSafeMutableRoaringBitmap());

    Map<String, PinotSegmentColumnReader> columnToReaderMap = new HashMap<>();
    List<String> primaryKeyColumns = Collections.singletonList(LOW_CARDINALITY_STRING_COL);
    String upsertComparisonColumn = LONG_COL_NAME;
    for (String primaryKeyColumn : primaryKeyColumns) {
      columnToReaderMap.put(primaryKeyColumn, new PinotSegmentColumnReader(immutableSegment, primaryKeyColumn));
    }
    columnToReaderMap
        .put(upsertComparisonColumn, new PinotSegmentColumnReader(immutableSegment, upsertComparisonColumn));
    int numTotalDocs = immutableSegment.getSegmentMetadata().getTotalDocs();
    int numPrimaryKeyColumns = primaryKeyColumns.size();
    Iterator<RecordInfo> recordInfoIterator =
        new Iterator<RecordInfo>() {
          private int _docId = 0;

          @Override
          public boolean hasNext() {
            return _docId < numTotalDocs;
          }

          @Override
          public RecordInfo next() {
            Object[] values = new Object[numPrimaryKeyColumns];
            for (int i = 0; i < numPrimaryKeyColumns; i++) {
              Object value = columnToReaderMap.get(primaryKeyColumns.get(i)).getValue(_docId);
              if (value instanceof byte[]) {
                value = new ByteArray((byte[]) value);
              }
              values[i] = value;
            }
            PrimaryKey primaryKey = new PrimaryKey(values);
            Object upsertComparisonValue = columnToReaderMap.get(upsertComparisonColumn).getValue(_docId);
            Preconditions.checkState(upsertComparisonValue instanceof Comparable,
                "Upsert comparison column: %s must be comparable", upsertComparisonColumn);
            return new RecordInfo(primaryKey, _docId++,
                (Comparable) upsertComparisonValue);
          }
        };

    partitionUpsertOffHeapMetadataManager.addSegment(immutableSegment, recordInfoIterator);
    return immutableSegment;
  }

  public static void close() {
//    for (IndexSegment indexSegment : _indexSegments) {
//      indexSegment.destroy();
//    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private static List<GenericRow> createTestData(int numRows) {
    Map<Integer, String> strings = new HashMap<>();
    List<GenericRow> rows = new ArrayList<>();

    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      row.putValue(SORTED_COL_NAME, numRows - i);
      row.putValue(INT_COL_NAME, (int) _supplier.getAsLong());
      row.putValue(LONG_COL_NAME, _supplier.getAsLong());
      row.putValue(NO_INDEX_INT_COL_NAME, (int) _supplier.getAsLong());
      row.putValue(RAW_INT_COL_NAME, (int) _supplier.getAsLong());
      row.putValue(RAW_STRING_COL_NAME,
          strings.computeIfAbsent((int) _supplier.getAsLong(), k -> UUID.randomUUID().toString()));
      row.putValue(NO_INDEX_STRING_COL, row.getValue(RAW_STRING_COL_NAME));
      row.putValue(LOW_CARDINALITY_STRING_COL, lowCardinalityValues[i % lowCardinalityValues.length]);
      rows.add(row);
    }
    return rows;
  }

  private static List<GenericRow> buildSegment(String segmentName)
      throws Exception {
    List<GenericRow> rows = createTestData(_numRows);
    List<FieldConfig> fieldConfigs = new ArrayList<>();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(fieldConfigs)
        .setNoDictionaryColumns(Arrays.asList(RAW_INT_COL_NAME, RAW_STRING_COL_NAME))
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL, Collections.emptyMap(), null, LONG_COL_NAME, HashFunction.NONE))
        .build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(SORTED_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(NO_INDEX_INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(RAW_INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(INT_COL_NAME, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_COL_NAME, FieldSpec.DataType.LONG)
        .addSingleValueDimension(RAW_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(NO_INDEX_STRING_COL, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LOW_CARDINALITY_STRING_COL, FieldSpec.DataType.STRING)
        .setPrimaryKeyColumns(Collections.singletonList(LOW_CARDINALITY_STRING_COL))
        .build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }

    return rows;
  }
}
