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
package org.apache.pinot.perf;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.upsert.IPartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManagerFactory;
import org.apache.pinot.segment.local.upsert.PartitionUpsertRocksDBMetadataManager;
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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.roaringbitmap.PeekableIntIterator;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgs = {"-server", "-Xmx8G", "-XX:MaxDirectMemorySize=16G"})
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
public class BenchmarkUpsertAddRecord {

  @Param({"ON_HEAP", "ON_HEAP_SERDE", "ROCKSDB", "OFF_HEAP", "MAPDB", "CHRONICLE_MAP", "H2", "SQLITE"})
  private String _metadataStoreType;

  @Param({"1000"})
  private int _numRows;

  @Param({"10"})
  private int _numSegments;

  @Param({"5000000"})
  private int _keyCardinality;

  @Param({"10"})
  private int _numRuns;

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "BenchmarkUpsertAddRecord");
  private static final String TABLE_NAME = "MyTable";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String LONG_COL_NAME = "LONG_COL"; // used as comparison column
  private static final String SORTED_COL_NAME = "SORTED_COL";
  private static final String RAW_INT_COL_NAME = "RAW_INT_COL";
  private static final String RAW_STRING_COL_NAME = "RAW_STRING_COL";
  private static final String NO_INDEX_INT_COL_NAME = "NO_INDEX_INT_COL";
  private static final String NO_INDEX_STRING_COL = "NO_INDEX_STRING_COL";
  private static final String LOW_CARDINALITY_STRING_COL = "LOW_CARDINALITY_STRING_COL"; // used as key column

  private List<IndexSegment> _indexSegments;
  private Map<IPartitionUpsertMetadataManager, List<IndexSegment>> _indexSegmentsPerImpl;
  private List<RecordInfo> _testData = new ArrayList<>();
  private LongSupplier _supplier;
  private String _scenario = "EXP(0.5)";
  private IPartitionUpsertMetadataManager _partitionUpsertMetadataManager;


  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkUpsertAddRecord.class.getSimpleName());
    if (args.length > 0 && args[0].equals("jfr")) {
      opt = opt.addProfiler(JavaFlightRecorderProfiler.class)
          .jvmArgsAppend("-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints");
    }
    new Runner(opt.build()).run();
  }

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    _testData.clear();

    _indexSegments = new ArrayList<>();
    _indexSegmentsPerImpl = new ConcurrentHashMap<>();
    PartitionUpsertMetadataManagerFactory.MetadataStore metadataStore = PartitionUpsertMetadataManagerFactory.MetadataStore.valueOf(_metadataStoreType);
    _partitionUpsertMetadataManager = PartitionUpsertMetadataManagerFactory.getPartitionUpsertMetadataManager(TABLE_NAME, 0, null, null, HashFunction.NONE, metadataStore);

    for (int i = 0; i < _numSegments; i++) {
      String name = "segment_" + i;
      buildSegment(name);
      enableUpsertOnSegmentAndMetadataManager(_partitionUpsertMetadataManager, name);
    }

    for(GenericRow genericRow: createTestData(_numRows)) {
      RecordInfo recordInfo = new RecordInfo(genericRow.getPrimaryKey(Collections.singletonList(LOW_CARDINALITY_STRING_COL)),  (int) _supplier.getAsLong() % _numRows, _supplier.getAsLong());
      _testData.add(recordInfo);
    }

  }

  @TearDown(Level.Trial)
  public void tearDown(BenchmarkParams benchmarkParams) {
    for (IndexSegment indexSegment : _indexSegments) {
      indexSegment.destroy();
    }

    //outputSegmentValidIds(benchmarkParams);

    for(List<IndexSegment> indexSegmentList: _indexSegmentsPerImpl.values()){
      for(IndexSegment indexSegment: indexSegmentList) {
        indexSegment.destroy();
      }
    }

    _partitionUpsertMetadataManager.close();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Benchmark
  public void upsertAddRecord(Blackhole blackhole) {
    try {
      for (IndexSegment indexSegment : _indexSegmentsPerImpl.get(_partitionUpsertMetadataManager)) {
        for(int i = 0 ; i < _numRuns ; i++) {
          for (RecordInfo recordInfo : _testData) {
            _partitionUpsertMetadataManager.addRecord(indexSegment, recordInfo);
            blackhole.consume(recordInfo);
          }
        }
      }
    } catch (Exception e){
      //e.printStackTrace();
    }
  }

  private List<GenericRow> createTestData(int numRows) {
    _supplier = Distribution.createLongSupplier(42, _scenario);
    Map<Integer, String> strings = new HashMap<>();
    List<GenericRow> rows = new ArrayList<>();
    String[] lowCardinalityValues = IntStream.range(0, _keyCardinality).mapToObj(i -> "value" + i)
        .toArray(String[]::new);

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

  private List<GenericRow> buildSegment(String segmentName)
      throws Exception {
    List<GenericRow> rows = createTestData(_numRows);
    List<FieldConfig> fieldConfigs = new ArrayList<>();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(fieldConfigs)
        .setNoDictionaryColumns(Arrays.asList(RAW_INT_COL_NAME, RAW_STRING_COL_NAME))
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL, Collections.emptyMap(), null, LONG_COL_NAME,
            HashFunction.NONE))
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

  private void enableUpsertOnSegmentAndMetadataManager(IPartitionUpsertMetadataManager partitionUpsertMetadataManager, String name)
      throws Exception {
    ImmutableSegmentImpl immutableSegment  = (ImmutableSegmentImpl) ImmutableSegmentLoader.load(new File(INDEX_DIR,
        name), new IndexLoadingConfig());
    immutableSegment.enableUpsert(null, new ThreadSafeMutableRoaringBitmap());
    //addSegmentToUpsertMetadata(immutableSegment, partitionUpsertMetadataManager);
    _indexSegmentsPerImpl.compute(partitionUpsertMetadataManager, (key, value) -> {
      if(value == null || value.isEmpty()) {
        List<IndexSegment> indexSegmentList = new ArrayList<>();
        indexSegmentList.add(immutableSegment);
        return indexSegmentList;
      } else {
        value.add(immutableSegment);
        return value;
      }
    });
  }

  private static ImmutableSegmentImpl addSegmentToUpsertMetadata(ImmutableSegmentImpl immutableSegment,
      IPartitionUpsertMetadataManager partitionUpsertOffHeapMetadataManager)
      throws Exception {
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

  private void outputSegmentValidIds(BenchmarkParams benchmarkParams) {
    String dirPath = Joiner.on("/").join(System.getProperty("user.dir"), benchmarkParams.getBenchmark());
    File dir = new File(dirPath);
    dir.mkdirs();

    for(Map.Entry<IPartitionUpsertMetadataManager, List<IndexSegment>> entry: _indexSegmentsPerImpl.entrySet()) {
      File file = new File(dirPath, entry.getKey().getClass().getSimpleName() + ".txt");
      try {
        FileWriter fileWriter = new FileWriter(file);
        for (IndexSegment indexSegment : entry.getValue()) {
          ImmutableSegmentImpl immutableSegment = (ImmutableSegmentImpl) indexSegment;
          String name = immutableSegment.getSegmentName();
          PeekableIntIterator intIterator = immutableSegment.getValidDocIds().getMutableRoaringBitmap().getIntIterator();
          GenericRow reuse = new GenericRow();

          while (intIterator.hasNext()) {
            int docId = intIterator.next();
            GenericRow record = immutableSegment.getRecord(docId, reuse);
            String metadata = Joiner.on("::").join(name, docId, record.getValue(LOW_CARDINALITY_STRING_COL), record.getValue(LONG_COL_NAME));
            //System.out.println(metadata);
            fileWriter.write(metadata + "\n");
          }
        }
        fileWriter.close();
      } catch (IOException e){
        System.out.println("Error occurred while creating output file: " + e.getMessage());
      }
    }
  }
}
