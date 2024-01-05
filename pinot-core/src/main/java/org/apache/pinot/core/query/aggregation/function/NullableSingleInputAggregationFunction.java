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

package org.apache.pinot.core.query.aggregation.function;

import it.unimi.dsi.fastutil.floats.FloatConsumer;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;


public abstract class NullableSingleInputAggregationFunction<I, F extends Comparable>
    extends BaseSingleInputAggregationFunction<I, F> {
  protected final boolean _nullHandlingEnabled;

  public NullableSingleInputAggregationFunction(ExpressionContext expression, boolean nullHandlingEnabled) {
    super(expression);
    _nullHandlingEnabled = nullHandlingEnabled;
  }


  <E> void forEachNotNullArray(int length, BlockValSet blockValSet, Function<BlockValSet, E[]> extract,
      Consumer<E> consumer) {
    forEachNotNullArray(length, blockValSet, extract, (i, value) -> consumer.accept(value));
  }

  <E> void forEachNotNullArray(int length, BlockValSet blockValSet, Function<BlockValSet, E[]> extract,
      ValueConsumer<E> consumer) {
    E[] values = extract.apply(blockValSet);
    if (!_nullHandlingEnabled) {
      for (int i = 0; i < length; i++) {
        consumer.accept(i, values[i]);
      }
    } else {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      int[] nulls = nullBitmap != null ? nullBitmap.toArray() : new int[0];

      int currentNull = 0;
      for (int i = 0; i < length; i++) {
        while (currentNull < nulls.length && nulls[currentNull] < i) {
          currentNull++;
        }
        if (currentNull >= nulls.length || nulls[currentNull] != i) {
          consumer.accept(i, values[i]);
        }
      }
    }
  }

  void forEachNotNullString(int length, BlockValSet blockValSet, Consumer<String> consumer) {
    forEachNotNullString(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullString(int length, BlockValSet blockValSet, ValueConsumer<String> consumer) {
    forEachNotNullArray(length, blockValSet, BlockValSet::getStringValuesSV, consumer);
  }

  <E> void forEachNotNull(int length, BlockValSet blockValSet, Function<BlockValSet, Iterable<E>> extract,
      Consumer<E> consumer) {
    Iterator<E> it = extract.apply(blockValSet).iterator();

    if (!_nullHandlingEnabled) {
      for (int i = 0; i < length && it.hasNext(); i++) {
        E object = it.next();
        consumer.accept(object);
      }
    } else {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      int[] nulls = nullBitmap != null ? nullBitmap.toArray() : new int[0];

      int currentNull = 0;
      for (int i = 0; i < length && it.hasNext(); i++) {
        E object = it.next();
        while (currentNull < nulls.length && nulls[currentNull] < i) {
          currentNull++;
        }
        if (currentNull >= nulls.length || nulls[currentNull] != i) {
          consumer.accept(object);
        }
      }
    }
  }

  <E> void forEachNotNullBytes(int length, BlockValSet blockValSet, Consumer<byte[]> consumer) {
    forEachNotNullBytes(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  <E> void forEachNotNullBytes(int length, BlockValSet blockValSet, ValueConsumer<byte[]> consumer) {
    byte[][] values = blockValSet.getBytesValuesSV();
    if (!_nullHandlingEnabled) {
      for (int i = 0; i < length; i++) {
        consumer.accept(i, values[i]);
      }
    } else {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      int[] nulls = nullBitmap != null ? nullBitmap.toArray() : new int[0];

      int currentNull = 0;
      for (int i = 0; i < length; i++) {
        while (currentNull < nulls.length && nulls[currentNull] < i) {
          currentNull++;
        }
        if (currentNull >= nulls.length || nulls[currentNull] != i) {
          consumer.accept(i, values[i]);
        }
      }
    }
  }

  void forEachNotNullFloat(int length, BlockValSet blockValSet, FloatConsumer consumer) {
    forEachNotNullFloat(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullFloat(int length, BlockValSet blockValSet, FloatValueConsumer consumer) {
    float[] values = blockValSet.getFloatValuesSV();
    if (!_nullHandlingEnabled) {
      for (int i = 0; i < length; i++) {
        consumer.accept(i, values[i]);
      }
    } else {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      int[] nulls = nullBitmap != null ? nullBitmap.toArray() : new int[0];

      int currentNull = 0;
      for (int i = 0; i < length; i++) {
        while (currentNull < nulls.length && nulls[currentNull] < i) {
          currentNull++;
        }
        if (currentNull >= nulls.length || nulls[currentNull] != i) {
          consumer.accept(i, values[i]);
        }
      }
    }
  }

  void forEachNotNullDouble(int length, BlockValSet blockValSet, DoubleConsumer consumer) {
    forEachNotNullDouble(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullDouble(int length, BlockValSet blockValSet, DoubleValueConsumer consumer) {
    double[] values = blockValSet.getDoubleValuesSV();
    if (!_nullHandlingEnabled) {
      for (int i = 0; i < length; i++) {
        consumer.accept(i, values[i]);
      }
    } else {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      int[] nulls = nullBitmap != null ? nullBitmap.toArray() : new int[0];

      int currentNull = 0;
      for (int i = 0; i < length; i++) {
        while (currentNull < nulls.length && nulls[currentNull] < i) {
          currentNull++;
        }
        if (currentNull >= nulls.length || nulls[currentNull] != i) {
          consumer.accept(i, values[i]);
        }
      }
    }
  }

  void forEachNotNullLong(int length, BlockValSet blockValSet, LongConsumer consumer) {
    forEachNotNullLong(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullLong(int length, BlockValSet blockValSet, LongValueConsumer consumer) {
    long[] values = blockValSet.getLongValuesSV();
    if (!_nullHandlingEnabled) {
      for (int i = 0; i < length; i++) {
        consumer.accept(i, values[i]);
      }
    } else {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      int[] nulls = nullBitmap != null ? nullBitmap.toArray() : new int[0];

      int currentNull = 0;
      for (int i = 0; i < length; i++) {
        while (currentNull < nulls.length && nulls[currentNull] < i) {
          currentNull++;
        }
        if (currentNull >= nulls.length || nulls[currentNull] != i) {
          consumer.accept(i, values[i]);
        }
      }
    }
  }

  void forEachNotNullDictId(int length, BlockValSet blockValSet, IntConsumer consumer) {
    forEachNotNullDictId(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullDictId(int length, BlockValSet blockValSet, IntValueConsumer consumer) {
    int[] values = blockValSet.getDictionaryIdsSV();
    if (!_nullHandlingEnabled) {
      for (int i = 0; i < length; i++) {
        consumer.accept(i, values[i]);
      }
    } else {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      int[] nulls = nullBitmap != null ? nullBitmap.toArray() : new int[0];

      int currentNull = 0;
      for (int i = 0; i < length; i++) {
        while (currentNull < nulls.length && nulls[currentNull] < i) {
          currentNull++;
        }
        if (currentNull >= nulls.length || nulls[currentNull] != i) {
          consumer.accept(i, values[i]);
        }
      }
    }
  }

  void forEachNotNullInt(int length, BlockValSet blockValSet, IntConsumer consumer) {
    forEachNotNullDictId(length, blockValSet, (i, value) -> consumer.accept(value));
  }

  void forEachNotNullInt(int length, BlockValSet blockValSet, IntValueConsumer consumer) {
    int[] values = blockValSet.getIntValuesSV();
    if (!_nullHandlingEnabled) {
      for (int i = 0; i < length; i++) {
        consumer.accept(i, values[i]);
      }
    } else {
      RoaringBitmap nullBitmap = blockValSet.getNullBitmap();
      int[] nulls = nullBitmap != null ? nullBitmap.toArray() : new int[0];

      int currentNull = 0;
      for (int i = 0; i < length; i++) {
        while (currentNull < nulls.length && nulls[currentNull] < i) {
          currentNull++;
        }
        if (currentNull >= nulls.length || nulls[currentNull] != i) {
          consumer.accept(i, values[i]);
        }
      }
    }
  }

  @FunctionalInterface
  interface DoubleValueConsumer {
    void accept(int index, double value);
  }

  @FunctionalInterface
  interface FloatValueConsumer {
    void accept(int index, float value);
  }

  @FunctionalInterface
  interface LongValueConsumer {
    void accept(int index, long value);
  }

  @FunctionalInterface
  interface IntValueConsumer {
    void accept(int index, int value);
  }

  @FunctionalInterface
  interface ValueConsumer<E> {
    void accept(int index, E value);
  }
}
