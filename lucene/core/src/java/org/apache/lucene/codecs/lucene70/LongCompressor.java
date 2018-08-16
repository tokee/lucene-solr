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
package org.apache.lucene.codecs.lucene70;

import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Utility class for generating compressed read-only in-memory representations of long-arrays.
 * The representation is optimized towards random access.
 * The representation always applies delta-to-minvalue and greatest-common-divisor compression.
 * Depending on the number of 0-entries and the length of the array, a sparse representation
 * is used, using rank to improve access speed. This can be turned off.
 */
// TODO: Consider how negative numbers are handled and if they should be supported at all
public class LongCompressor {

  /**
   * The minimum amount of total values in the data set for sparse to be active.
   */
  public static final int DEFAULT_MIN_TOTAL_VALUES_FOR_SPARSE = 10_000;

  /**
   * The minimum total amount of zero values in the data set for sparse to be active.
   */
  public static final int DEFAULT_MIN_ZERO_VALUES_FOR_SPARSE = 500;
  /**
   * The minimum fraction of the data set that must be zero for sparse to be active.
   */
  public static final double DEFAULT_MIN_ZERO_VALUES_FRACTION_FOR_SPARSE = 0.2;

  public static PackedInts.Reader compress(long[] values) {
    return compress(values, values.length);
  }

  public static PackedInts.Reader compress(long[] values, int length) {
    return compress(values, values.length, true);
  }

  public static PackedInts.Reader compress(long[] values, int length, boolean allowSparse) {
    return compress(values, length, allowSparse,
        DEFAULT_MIN_TOTAL_VALUES_FOR_SPARSE,
        DEFAULT_MIN_ZERO_VALUES_FOR_SPARSE,
        DEFAULT_MIN_ZERO_VALUES_FRACTION_FOR_SPARSE);
  }

  public static PackedInts.Reader compress(
      long[] values, int length, boolean allowSparse,
      int minTotalSparse, int minZeroSparse, double minZeroFractionSparse) {
    if (length == 0) {
      return PackedInts.getMutable(0, 1, PackedInts.DEFAULT);
    }

    final long min = getMin(values, length);
    final long gcd = getGCD(values, length, min);
    final long maxCompressed = getMax(values, length, min, gcd);
    PackedInts.Mutable inner =
        PackedInts.getMutable(length, PackedInts.bitsRequired(maxCompressed), PackedInts.DEFAULT);
    for (int i = 0 ; i < length ; i++) {
      inner.set(i, (values[i]-min)/gcd);
    }

    //final boolean useSparse = isSparseCandidate(values, length, min, allowSparse,
    //    minTotalSparse, minZeroSparse, minZeroFractionSparse);
    // TODO (Toke): Implement sparse representation with rank (see the sparse faceting project for easy-to-adapt code)
    
    return new CompressedReader(inner, min, gcd);
  }

  private static boolean isSparseCandidate(
      long[] values, int length, long zeroValue, boolean allowSparse,
      int minTotalSparse, int minZeroSparse, double minZeroFractionSparse) {
    if (!allowSparse || minTotalSparse < length) {
      return false;
    }
    int zeroCount = 0;
    for (int i = 0 ; i < length ; i++) {
      if (values[i] == zeroValue) {
        zeroCount++;
      }
    }
    return minZeroSparse < zeroCount && minZeroFractionSparse < 1.0*zeroCount/length;
  }

  static class CompressedReader extends PackedInts.Reader {
    private final PackedInts.Reader inner;
    final long min;
    final long gcd;

    public CompressedReader(PackedInts.Reader inner, long min, long gcd) {
      this.inner = inner;
      this.min = min;
      this.gcd = gcd;
    }

    @Override
    public int size() {
      return inner.size();
    }

    @Override
    public long get(int docID) {
      return (inner.get(docID)+min)*gcd;
    }

    @Override
    public long ramBytesUsed() {
      return inner.ramBytesUsed();
    }
  }

  private static long getMin(long[] values, int length) {
    long min = Long.MAX_VALUE;
    for (int i = 0 ; i < length ; i++) {
      if (min > values[i]) {
        min = values[i];
      }
    }
    return min;
  }

  private static long getGCD(final long[] values, final int length, final long min) {
    // GCD-code adjusted form Lucene70DocValuesConsumer
    long gcd = values[0];

    for (int i = 1 ; i < length ; i++) {
      long value = values[i]-min;

      if (value < Long.MIN_VALUE / 2 || value > Long.MAX_VALUE / 2) {
        // in that case v - minValue might overflow and make the GCD computation return
        // wrong results. Since these extreme values are unlikely, we just discard
        // GCD computation for them
        gcd = 1;
      } else { // minValue needs to be set first
        gcd = MathUtil.gcd(gcd, value);
      }

      if (gcd == 1) {
        break;
      }
    }
    return gcd;
  }

  private static long getMax(final long[] values, final int length, final long min, final long gcd) {
    long max = Long.MIN_VALUE;
    for (int i = 0 ; i < length ; i++) {
      long value = (values[i]-min)/gcd;
      if (value > max) {
        max = value;
      }
    }
    return max;
  }

}
