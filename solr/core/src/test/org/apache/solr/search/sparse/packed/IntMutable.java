/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.search.sparse.packed;

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Trivial int-backed implementation. It would be better to use {@link org.apache.lucene.util.packed.Direct32},
 * but that is packet private.
 */
public class IntMutable extends PackedInts.Mutable {
  protected final int valueCount;
  protected final int bitsPerValue;
  final int[] values;

  public IntMutable(int[] values) {
    this.values = values;
    this.valueCount = values.length;
    bitsPerValue = 32;
  }
  public IntMutable(int valueCount) {
    this(valueCount, 32);
  }
  public IntMutable(int valueCount, int bitsPerValue) {
    this.valueCount = valueCount;
    this.bitsPerValue = bitsPerValue;
    values = new int[valueCount];
  }

  @Override
  public final int getBitsPerValue() {
    return bitsPerValue;
  }

  @Override
  public final int size() {
    return valueCount;
  }

  @Override
  public long get(final int index) {
    return values[index] & 0xFFFFFFFFL;
  }

  @Override
  public void set(final int index, final long value) {
    values[index] = (int) (value);
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 2 * Integer.BYTES     // valueCount,bitsPerValue
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // values ref
        + RamUsageEstimator.sizeOf(values);
  }
}
