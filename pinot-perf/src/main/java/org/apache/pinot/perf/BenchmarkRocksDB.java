package org.apache.pinot.perf;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.JavaFlightRecorderProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompressionType;
import org.rocksdb.DataBlockIndexType;
import org.rocksdb.IndexShorteningMode;
import org.rocksdb.IndexType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;


@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 1, time = 10)
@Measurement(iterations = 2, time = 10)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkRocksDB {
  private static final int NUM_VALUES = 10_000_000;
  private static final Random RANDOM = new Random();

  private final byte[][] _keys = new byte[NUM_VALUES][];
  private final ByteBuffer[] _keysInt = new ByteBuffer[NUM_VALUES];

  private final byte[][] _values = new byte[NUM_VALUES][];

  @Param({"150000"})
  public int _cardinality;
  private Map<ByteBuffer, byte[]> _mapStore;
  private Map<ByteBuffer, byte[]> _treeMapStore;

  private RocksDB _rocksDB;
  private WriteOptions _writeOptions;

  @Setup
  public void setUp()
      throws Exception {
    for (int i = 0; i < NUM_VALUES; i++) {
      int val = RANDOM.nextInt(_cardinality);
      _keys[i] = toBytes(val);
      _keysInt[i] = ByteBuffer.wrap(_keys[i]);
      _values[i] = toBytes(RANDOM.nextInt());
    }

    _mapStore = new ConcurrentHashMap<>();
    _treeMapStore = new TreeMap<>();
    Options options = new Options();
    options.setCreateIfMissing(true);
    _rocksDB = RocksDB.open (options, Files.createTempDirectory("benchmark").toAbsolutePath().toString());
    _writeOptions = new WriteOptions();
    _writeOptions.setDisableWAL(true);

    for(int i = 0; i<NUM_VALUES/10;i++) {
      byte[] k = toBytes(RANDOM.nextInt(_cardinality));
      byte[] v = toBytes(RANDOM.nextInt());
      _rocksDB.put(k, v);
      _mapStore.put(ByteBuffer.wrap(k), v);
    }
  }

  @Benchmark
  public void benchmarkMap(Blackhole blackhole){
    for(int i = 0; i < NUM_VALUES; i++) {
      byte[] val = _mapStore.getOrDefault(_keysInt[i], new byte[]{});
      //byte[] op = _mapStore.put(_keys[i], _values[i]);
      blackhole.consume(val);
      //blackhole.consume(op);
    }
  }

  @Benchmark
  public void benchmarkRocks(Blackhole blackhole){
    for(int i = 0; i < NUM_VALUES; i++) {
      try {
         byte[] val = _rocksDB.get(_keys[i]);
        //_rocksDB.put(_writeOptions, _keys[i], _values[i]);
        blackhole.consume(val);
      } catch (Exception e) {
        //
      }
    }
  }

  public void benchmarkMapPut(Blackhole blackhole){
    for(int i = 0; i < NUM_VALUES; i++) {
      byte[] op = _mapStore.put(_keysInt[i], _values[i]);
      blackhole.consume(op);
    }
  }

  public void benchmarkRocksPut(Blackhole blackhole){
    for(int i = 0; i < NUM_VALUES; i++) {
      try {
        _rocksDB.put(_writeOptions, _keys[i], _values[i]);
        blackhole.consume(_values[i]);
      } catch (Exception e) {
        //
      }
    }
  }

  private byte[] toBytes(int i)
  {
    byte[] result = new byte[4];

    result[0] = (byte) (i >> 24);
    result[1] = (byte) (i >> 16);
    result[2] = (byte) (i >> 8);
    result[3] = (byte) (i /*>> 0*/);

    return result;
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkRocksDB.class.getSimpleName());
    if (args.length > 0 && args[0].equals("jfr")) {
      opt = opt.addProfiler(JavaFlightRecorderProfiler.class)
          .jvmArgsAppend("-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints");
    }
    new Runner(opt.build()).run();
  }

}
