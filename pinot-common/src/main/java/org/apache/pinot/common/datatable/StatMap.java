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
package org.apache.pinot.common.datatable;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * A map that stores statistics.
 *
 * Statistics must be keyed by an enum that implements {@link StatMap.Key}.
 *
 * A stat map efficiently store, serialize and deserialize these statistics.
 *
 * Serialization and deserialization is backward and forward compatible as long as the only change in the keys are:
 * <ul>
 *   <li>Adding new keys</li>
 *   <li>Change the name of the keys</li>
 * </ul>
 *
 * Any other change (like changing the type of key, changing their literal order are not supported or removing keys)
 * are backward incompatible changes.
 * @param <K>
 */
public class StatMap<K extends Enum<K> & StatMap.Key> {
  private final Family<K> _family;
  @Nullable
  private final int[] _intValues;
  @Nullable
  private final long[] _longValues;
  @Nullable
  private final boolean[] _booleanValues;
  /**
   * In Pinot 1.1.0 this can only store String values, but it is prepared to be able to store any kind of object in
   * the future.
   */
  @Nullable
  private final Object[] _referenceValues;

  public StatMap(Class<K> keyClass) {
    Family<K> family = Family.getFamily(keyClass);
    _family = family;
    _intValues = family.createIntValues();
    _longValues = family.createLongValues();
    _booleanValues = family.createBooleanValues();
    _referenceValues = family.createReferenceValues();
  }

  public int getInt(K key) {
    Preconditions.checkArgument(key.getType() == Type.INT);
    int index = _family.getIndex(key);
    assert _intValues != null : "Int values should not be null because " + key + " is of type INT";
    return _intValues[index];
  }

  public StatMap<K> merge(K key, int value) {
    if (key.getType() == Type.LONG) {
      merge(key, (long) value);
      return this;
    }
    Preconditions.checkArgument(key.getType() == Type.INT);
    int index = _family.getIndex(key);
    assert _intValues != null : "Int values should not be null because " + key + " is of type INT";
    _intValues[index] = key.merge(_intValues[index], value);
    return this;
  }

  public long getLong(K key) {
    if (key.getType() == Type.INT) {
      return getInt(key);
    }
    Preconditions.checkArgument(key.getType() == Type.LONG);
    int index = _family.getIndex(key);
    assert _longValues != null : "Long values should not be null because " + key + " is of type LONG";
    return _longValues[index];
  }

  public StatMap<K> merge(K key, long value) {
    Preconditions.checkArgument(key.getType() == Type.LONG);
    int index = _family.getIndex(key);
    assert _longValues != null : "Long values should not be null because " + key + " is of type LONG";
    _longValues[index] = key.merge(_longValues[index], value);
    return this;
  }

  public boolean getBoolean(K key) {
    Preconditions.checkArgument(key.getType() == Type.BOOLEAN);
    int index = _family.getIndex(key);
    assert _booleanValues != null : "Boolean values should not be null because " + key + " is of type BOOLEAN";
    return _booleanValues[index];
  }

  public StatMap<K> merge(K key, boolean value) {
    Preconditions.checkArgument(key.getType() == Type.BOOLEAN);
    int index = _family.getIndex(key);
    assert _booleanValues != null : "Boolean values should not be null because " + key + " is of type BOOLEAN";
    _booleanValues[index] = key.merge(_booleanValues[index], value);
    return this;
  }

  public String getString(K key) {
    Preconditions.checkArgument(key.getType() == Type.STRING);
    int index = _family.getIndex(key);
    assert _referenceValues != null : "Reference values should not be null because " + key + " is of type STRING";
    return (String) _referenceValues[index];
  }

  public StatMap<K> merge(K key, String value) {
    Preconditions.checkArgument(key.getType() == Type.STRING);
    int index = _family.getIndex(key);
    assert _referenceValues != null : "Reference values should not be null because " + key + " is of type STRING";
    _referenceValues[index] = key.merge((String) _referenceValues[index], value);
    return this;
  }

  /**
   * Returns the value associated with the key.
   *
   * Primitives will be boxed, so it is recommended to use the specific methods for each type.
   */
  public Object getAny(K key) {
    switch (key.getType()) {
      case BOOLEAN:
        return getBoolean(key);
      case INT:
        return getInt(key);
      case LONG:
        return getLong(key);
      case STRING:
        return getString(key);
      default:
        throw new IllegalArgumentException("Unsupported type: " + key.getType());
    }
  }

  /**
   * Modifies this object to merge the values of the other object.
   *
   * Numbers will be added, booleans will be ORed, and strings will be set if they are null.
   *
   * @param other The object to merge with. This argument will not be modified.
   * @return this object once it is modified.
   */
  public StatMap<K> merge(StatMap<K> other) {
    Preconditions.checkState(_family._keyClass.equals(other._family._keyClass),
        "Different key classes %s and %s", _family._keyClass, other._family._keyClass);
    Preconditions.checkState(_family._numIntValues == other._family._numIntValues,
        "Different number of int values");
    for (int i = 0; i < _family._numIntValues; i++) {
      assert _intValues != null : "Int values should not be null because there are int keys";
      assert other._intValues != null : "Int values should not be null because there are int keys";
      K key = _family.getKey(i, Type.INT);
      _intValues[i] = key.merge(_intValues[i], other._intValues[i]);
    }

    Preconditions.checkState(_family._numLongValues == other._family._numLongValues,
        "Different number of long values");
    for (int i = 0; i < _family._numLongValues; i++) {
      assert _longValues != null : "Long values should not be null because there are long keys";
      assert other._longValues != null : "Long values should not be null because there are long keys";
      K key = _family.getKey(i, Type.LONG);
      _longValues[i] = key.merge(_longValues[i], other._longValues[i]);
    }

    Preconditions.checkState(_family._numBooleanValues == other._family._numBooleanValues,
        "Different number of boolean values");
    for (int i = 0; i < _family._numBooleanValues; i++) {
      assert _booleanValues != null : "Boolean values should not be null because there are boolean keys";
      assert other._booleanValues != null : "Boolean values should not be null because there are boolean keys";
      K key = _family.getKey(i, Type.BOOLEAN);
      _booleanValues[i] = key.merge(_booleanValues[i], other._booleanValues[i]);
    }

    Preconditions.checkState(_family._numReferenceValues == other._family._numReferenceValues,
        "Different number of reference values");
    for (int i = 0; i < _family._numReferenceValues; i++) {
      assert _referenceValues != null : "Reference values should not be null because there are reference keys";
      assert other._referenceValues != null : "Reference values should not be null because there are reference keys";
      if (_referenceValues[i] == null) {
        _referenceValues[i] = other._referenceValues[i];
      } else {
        K key = _family.getKey(i, Type.STRING);
        _referenceValues[i] = key.merge((String) _referenceValues[i], (String) other._referenceValues[i]);
      }
    }
    return this;
  }

  public StatMap<K> merge(DataInput input)
      throws IOException {
    byte serializedKeys = input.readByte();

    for (byte i = 0; i < serializedKeys; i++) {
      int ordinal = input.readByte();
      K key = _family._keys[ordinal];
      switch (key.getType()) {
        case BOOLEAN:
          assert _booleanValues != null : "Boolean values should not be null because there are boolean keys";
          _booleanValues[ordinal] = true;
          break;
        case INT:
          assert _intValues != null : "Int values should not be null because there are int keys";
          _intValues[ordinal] = key.merge(_intValues[ordinal], input.readInt());
          break;
        case LONG:
          assert _longValues != null : "Long values should not be null because there are long keys";
          _longValues[ordinal] = key.merge(_longValues[ordinal], input.readLong());
          break;
        case STRING:
          assert _referenceValues != null : "Reference values should not be null because there are reference keys";
          _referenceValues[ordinal] = key.merge((String) _referenceValues[ordinal], input.readUTF());
          break;
        default:
          throw new IllegalStateException("Unknown type " + key.getType());
      }
    }
    return this;
  }

  public ObjectNode asJson() {
    ObjectNode node = JsonUtils.newObjectNode();

    for (int i = 0; i < _family._keys.length; i++) {
      K key = _family._keys[i];
      switch (key.getType()) {
        case BOOLEAN:
          if (_booleanValues != null && _booleanValues[i]) {
            node.put(key.getStatName(), true);
          }
          break;
        case INT:
          if (_intValues != null && _intValues[i] != 0) {
            node.put(key.getStatName(), _intValues[i]);
          }
          break;
        case LONG:
          if (_longValues != null && _longValues[i] != 0) {
            node.put(key.getStatName(), _longValues[i]);
          }
          break;
        case STRING:
          if (_referenceValues != null && _referenceValues[i] != null) {
            node.put(key.getStatName(), (String) _referenceValues[i]);
          }
          break;
        default:
          throw new IllegalStateException("Unknown type " + key.getType());
      }
    }

    return node;
  }

  public int size() {
    int size = 0;
    if (_intValues != null) {
      for (int i = 0; i < _intValues.length; i++) {
        if (_intValues[i] != 0) {
          size++;
        }
      }
    }
    if (_longValues != null) {
      for (int i = 0; i < _longValues.length; i++) {
        if (_longValues[i] != 0) {
          size++;
        }
      }
    }
    if (_booleanValues != null) {
      for (int i = 0; i < _booleanValues.length; i++) {
        if (_booleanValues[i]) {
          size++;
        }
      }
    }
    if (_referenceValues != null) {
      for (int i = 0; i < _referenceValues.length; i++) {
        if (_referenceValues[i] != null) {
          size++;
        }
      }
    }
    return size;
  }

  public void serialize(DataOutput output)
      throws IOException {

    output.writeByte(size());

    for (int i = 0; i < _family._keys.length; i++) {
      K key = _family._keys[i];
      byte index = _family.getIndex(key);
      switch (key.getType()) {
        case BOOLEAN: {
          if (_booleanValues != null && _booleanValues[index]) {
            output.writeByte(i);
          }
          break;
        }
        case INT: {
          if (_intValues != null && _intValues[index] != 0) {
            output.writeByte(i);
            output.writeInt(_intValues[index]);
          }
          break;
        }
        case LONG: {
          if (_longValues != null && _longValues[index] != 0) {
            output.writeByte(i);
            output.writeLong(_longValues[index]);
          }
          break;
        }
        case STRING: {
          if (_referenceValues != null && _referenceValues[index] != null) {
            output.writeByte(i);
            output.writeUTF((String) _referenceValues[index]);
          }
          break;
        }
        default:
          throw new IllegalStateException("Unknown type " + key.getType());
      }
    }
  }

  public static <K extends Enum<K> & Key> StatMap<K> deserialize(DataInput input, Class<K> keyClass)
      throws IOException {
    StatMap<K> result = new StatMap<>(keyClass);
    result.merge(input);
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StatMap<?> statMap = (StatMap<?>) o;
    return Objects.equals(_family, statMap._family) && Arrays.equals(_intValues, statMap._intValues)
        && Arrays.equals(_longValues, statMap._longValues) && Arrays.equals(_booleanValues, statMap._booleanValues)
        && Arrays.equals(_referenceValues, statMap._referenceValues);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(_family);
    result = 31 * result + Arrays.hashCode(_intValues);
    result = 31 * result + Arrays.hashCode(_longValues);
    result = 31 * result + Arrays.hashCode(_booleanValues);
    result = 31 * result + Arrays.hashCode(_referenceValues);
    return result;
  }

  @Override
  public String toString() {
    return asJson().toString();
  }

  public Class<K> getKeyClass() {
    return _family._keyClass;
  }

  public boolean isEmpty() {
    if (_intValues != null) {
      for (int intValue : _intValues) {
        if (intValue != 0) {
          return false;
        }
      }
    }
    if (_longValues != null) {
      for (long longValue : _longValues) {
        if (longValue != 0) {
          return false;
        }
      }
    }
    if (_booleanValues != null) {
      for (boolean booleanValue : _booleanValues) {
        if (booleanValue) {
          return false;
        }
      }
    }
    if (_referenceValues != null) {
      for (Object referenceValue : _referenceValues) {
        if (referenceValue != null) {
          return false;
        }
      }
    }
    return true;
  }

  private static class Family<K extends Enum<K> & Key> {
    private final Class<K> _keyClass;
    private final int _numIntValues;
    private final int _numLongValues;
    private final int _numBooleanValues;
    private final int _numReferenceValues;
    private final int _maxIndex;
    private final byte[] _keyToIndexMap;
    private final K[] _keys;

    private static final ConcurrentHashMap<Class<? extends Enum>, Family> FAMILY_MAP = new ConcurrentHashMap<>();

    private Family(Class<K> keyClass) {
      _keyClass = keyClass;
      _keys = keyClass.getEnumConstants();
      if (_keys.length > Byte.MAX_VALUE) {
        // In fact, we could support up to 256 keys if we interpret the byte as unsigned
        throw new IllegalArgumentException("Too many keys: " + _keys.length);
      }
      byte numIntValues = 0;
      byte numLongValues = 0;
      byte numBooleanValues = 0;
      byte numReferenceValues = 0;
      _keyToIndexMap = new byte[_keys.length];
      for (K key : _keys) {
        int ordinal = key.ordinal();
        switch (key.getType()) {
          case BOOLEAN:
            _keyToIndexMap[ordinal] = numBooleanValues++;
            break;
          case INT:
            _keyToIndexMap[ordinal] = numIntValues++;
            break;
          case LONG:
            _keyToIndexMap[ordinal] = numLongValues++;
            break;
          case STRING:
            _keyToIndexMap[ordinal] = numReferenceValues++;
            break;
          default:
            throw new IllegalStateException();
        }
      }
      _numIntValues = numIntValues;
      _numLongValues = numLongValues;
      _numBooleanValues = numBooleanValues;
      _numReferenceValues = numReferenceValues;
      _maxIndex = Math.max(numIntValues, Math.max(numLongValues, Math.max(numBooleanValues, numReferenceValues)));
    }

    @SuppressWarnings("unchecked")
    public static <K extends Enum<K> & Key> Family<K> getFamily(Class<K> keyClass) {
      return FAMILY_MAP.computeIfAbsent(keyClass, k -> new Family(k));
    }

    @Nullable
    private int[] createIntValues() {
      if (_numIntValues == 0) {
        return null;
      }
      return new int[_numIntValues];
    }

    @Nullable
    private long[] createLongValues() {
      if (_numLongValues == 0) {
        return null;
      }
      return new long[_numLongValues];
    }

    @Nullable
    private boolean[] createBooleanValues() {
      if (_numBooleanValues == 0) {
        return null;
      }
      return new boolean[_numBooleanValues];
    }

    @Nullable
    private Object[] createReferenceValues() {
      if (_numReferenceValues == 0) {
        return null;
      }
      return new Object[_numReferenceValues];
    }

    private byte getIndex(K key) {
      if (key.getClass() != _keyClass) {
        throw new IllegalArgumentException("Key " + key + " is not from family " + _keyClass);
      }
      return _keyToIndexMap[key.ordinal()];
    }

    private K getKey(int index, Type type) {
      for (int i = 0; i < _keyToIndexMap.length; i++) {
        int actualIndex = _keyToIndexMap[i];
        if (actualIndex == index && _keys[i].getType() == type) {
          return _keys[i];
        }
      }
      throw new IllegalArgumentException("Unknown key for index " + index + " and type " + type);
    }

    /**
     * Returns a list with the keys of a given type.
     *
     * The iteration order is the ascending order of calling {@code #getIndex(K)}.
     */
    private List<K> getKeysOfType(Type type) {
      return Arrays.stream(_keyClass.getEnumConstants())
          .filter(key -> key.getType() == type)
          .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Family<?> family = (Family<?>) o;
      return Objects.equals(_keyClass, family._keyClass);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_keyClass);
    }

    @Override
    public String toString() {
      return "Family{" + "_keyClass=" + _keyClass + '}';
    }
  }

  public interface Key {
    String name();

    /**
     * The name of the stat used to report it. Names must be unique on the same key family.
     */
    default String getStatName() {
      String name = name();
      StringBuilder result = new StringBuilder();
      boolean capitalizeNext = false;

      for (char c : name.toCharArray()) {
        if (c == '_') {
          capitalizeNext = true;
        } else {
          if (capitalizeNext) {
            result.append(c);
            capitalizeNext = false;
          } else {
            result.append(Character.toLowerCase(c));
          }
        }
      }

      return result.toString();
    }

    default int merge(int value1, int value2) {
      return value1 + value2;
    }

    default long merge(long value1, long value2) {
      return value1 + value2;
    }

    default double merge(double value1, double value2) {
      return value1 + value2;
    }

    default boolean merge(boolean value1, boolean value2) {
      return value1 || value2;
    }

    default String merge(@Nullable String value1, @Nullable String value2) {
      return value2 != null ? value2 : value1;
    }

    /**
     * The type of the values associated to this key.
     */
    Type getType();

    default boolean includeDefaultInJson() {
      return false;
    }
  }

  public enum Type {
    BOOLEAN,
    INT,
    LONG,
    STRING
  }
}
