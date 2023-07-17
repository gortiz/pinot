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
package org.apache.pinot.core.data.table;

import java.util.Arrays;


@SuppressWarnings("EqualsHashCode")
public abstract class AbstractKey {
  public abstract Object[] getValues();

  @Override
  public int hashCode() {
    int result = 1;

    for (Object element : getValues()) {
      result = 31 * result + (element == null ? 0 : element.hashCode());
    }

    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof AbstractKey)) {
      return false;
    }
    return equals(this, ((AbstractKey) obj));
  }

  public static boolean equals(AbstractKey k1, AbstractKey k2) {
    if (k1 == k2) {
      return true;
    }
    if (k1 instanceof LightweightKey) {
      return ((LightweightKey) k1).lightweightEquals(k2);
    }
    if (k2 instanceof LightweightKey) {
      return ((LightweightKey) k2).lightweightEquals(k1);
    }
    Object[] val1 = k1.getValues();
    Object[] val2 = k2.getValues();
    return Arrays.equals(val1, val2);
  }
}
