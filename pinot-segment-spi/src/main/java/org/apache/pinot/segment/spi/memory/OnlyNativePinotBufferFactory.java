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
package org.apache.pinot.segment.spi.memory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;


public abstract class OnlyNativePinotBufferFactory implements PinotBufferFactory {

  @Override
  public PinotDataBuffer allocateDirect(long size, ByteOrder byteOrder) {
    return correctOrder(allocateDirect(size), byteOrder);
  }

  protected abstract PinotDataBuffer allocateDirect(long size);

  @Override
  public PinotDataBuffer mapFile(File file, boolean readOnly, long offset, long size, ByteOrder byteOrder)
      throws IOException {
    return correctOrder(mapFile(file, readOnly, offset, size), byteOrder);
  }

  protected abstract PinotDataBuffer mapFile(File file, boolean readOnly, long offset, long size) throws IOException;

  private PinotDataBuffer correctOrder(PinotDataBuffer nativeBuffer, ByteOrder byteOrder) {
    return byteOrder == ByteOrder.nativeOrder() ? nativeBuffer : new NonNativePinotDataBuffer(nativeBuffer);
  }
}
