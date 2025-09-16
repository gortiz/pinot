/*
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
package org.apache.pinot.core.util;

import java.util.AbstractList;
import java.util.ArrayList;


/// A list optimized to support only appending elements to the end of the list.
///
/// By doing that, if the list is large, we don't need to allocate a large continuous array to hold all the elements
/// or copy the elements when the array needs to grow. Instead, we allocate small fixed-size arrays (pages
/// of size PAGE_SIZE) and link them together.
///
/// This class is also not thread-safe.
public class AppendOnlyList<E> extends AbstractList<E> {
  private static final int PAGE_SIZE = 1024;
  private final ArrayList<E[]> _pages = new ArrayList<>(PAGE_SIZE);
  private E[] _currentPage;
  private int _indexInCurrentPage;

  public AppendOnlyList() {
    addPage();
  }

  private void addPage() {
    _currentPage = (E[]) new Object[PAGE_SIZE];
    _pages.add(_currentPage);
    _indexInCurrentPage = 0;
  }

  @Override
  public boolean add(E element) {
    if (_indexInCurrentPage >= PAGE_SIZE) {
      addPage();
    }
    _currentPage[_indexInCurrentPage++] = element;
    return true;
  }

  @Override
  public E get(int index) {
    if (index < 0 || index >= size()) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
    }
    int pageIndex = index / PAGE_SIZE;
    int indexInPage = index % PAGE_SIZE;
    return _pages.get(pageIndex)[indexInPage];
  }

  @Override
  public int size() {
    return (_pages.size() - 1) * PAGE_SIZE + _indexInCurrentPage;
  }
}
