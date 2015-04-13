package org.apache.lucene.util.packed;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.Incrementable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Space optimized random access capable array of values with a fixed number of
 * bits/value. Values are packed contiguously.
 * </p><p>
 * This implementation used an {@link AtomicLongArray} as backing structure.
 * It provides thread-safe {@link #inc(int)} and {@link #set(int, long)} by opportunistic
 * updates. With low contention this is very effective; with high contention, performance
 * drops quickly.
 * </p><p>
 * Important: Only {@link #inc(int)} and {@link #set(int, long)} are thread safe.
 * </p><p>
 * The class {@link Packed64SingleBlock} is used as template as using Atomics
 * for collision handling requires update to the underlying structure to be
 * confined to a single Atomic.  
 *
 */
// TODO: Add support for bulk get & set
abstract class PackedOpportunistic extends PackedInts.MutableImpl implements Incrementable {

  public static final int MAX_SUPPORTED_BITS_PER_VALUE = 32;
  // TODO: Extend this list
  private static final int[] SUPPORTED_BITS_PER_VALUE = new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 21, 32};

  public static boolean isSupported(int bitsPerValue) {
    return Arrays.binarySearch(SUPPORTED_BITS_PER_VALUE, bitsPerValue) >= 0;
  }

  private static int requiredCapacity(int valueCount, int valuesPerBlock) {
    return valueCount / valuesPerBlock
        + (valueCount % valuesPerBlock == 0 ? 0 : 1);
  }

  final AtomicLongArray blocks;
  final long incOverflow;

  PackedOpportunistic(int valueCount, int bitsPerValue) {
    super(valueCount, bitsPerValue);
    assert isSupported(bitsPerValue);
    final int valuesPerBlock = 64 / bitsPerValue;
    blocks = new AtomicLongArray(requiredCapacity(valueCount, valuesPerBlock));
    incOverflow = (long) Math.pow(2, bitsPerValue);
  }

  @Override
  public void clear() {
    // TODO: Consider if simple re-allocation is faster than loop-based zeroing
    for (int i = 0 ; i < blocks.length() ; i++) {
      blocks.set(i, 0L);
    }
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
        + 2 * RamUsageEstimator.NUM_BYTES_INT     // valueCount,bitsPerValue
        + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // blocks ref
        + RamUsageEstimator.sizeOf(blocks);
  }

  // decoder.decode needs raw longs, which we do not have access to
/*  public int get(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    len = Math.min(len, valueCount - index);
    assert off + len <= arr.length;

    final int originalIndex = index;

    // go to the next block boundary
    final int valuesPerBlock = 64 / bitsPerValue;
    final int offsetInBlock = index % valuesPerBlock;
    if (offsetInBlock != 0) {
      for (int i = offsetInBlock; i < valuesPerBlock && len > 0; ++i) {
        arr[off++] = get(index++);
        --len;
      }
      if (len == 0) {
        return index - originalIndex;
      }
    }

    // bulk get
    assert index % valuesPerBlock == 0;
    final PackedInts.Decoder decoder = BulkOperation.of(PackedInts.Format.PACKED_SINGLE_BLOCK, bitsPerValue);
    assert decoder.longBlockCount() == 1;
    assert decoder.longValueCount() == valuesPerBlock;
    final int blockIndex = index / valuesPerBlock;
    final int nblocks = (index + len) / valuesPerBlock - blockIndex;
    decoder.decode(blocks, blockIndex, arr, off, nblocks);
    final int diff = nblocks * valuesPerBlock;
    index += diff; len -= diff;

    if (index > originalIndex) {
      // stay at the block boundary
      return index - originalIndex;
    } else {
      // no progress so far => already at a block boundary but no full block to
      // get
      assert index == originalIndex;
      return super.get(index, arr, off, len);
    }
  }
  */

  // op.encode needs raw longs, which we do not have access to
/*  @Override
  public int set(int index, long[] arr, int off, int len) {
    assert len > 0 : "len must be > 0 (got " + len + ")";
    assert index >= 0 && index < valueCount;
    len = Math.min(len, valueCount - index);
    assert off + len <= arr.length;

    final int originalIndex = index;

    // go to the next block boundary
    final int valuesPerBlock = 64 / bitsPerValue;
    final int offsetInBlock = index % valuesPerBlock;
    if (offsetInBlock != 0) {
      for (int i = offsetInBlock; i < valuesPerBlock && len > 0; ++i) {
        set(index++, arr[off++]);
        --len;
      }
      if (len == 0) {
        return index - originalIndex;
      }
    }

    // bulk set
    assert index % valuesPerBlock == 0;
    final BulkOperation op = BulkOperation.of(PackedInts.Format.PACKED_SINGLE_BLOCK, bitsPerValue);
    assert op.longBlockCount() == 1;
    assert op.longValueCount() == valuesPerBlock;
    final int blockIndex = index / valuesPerBlock;
    final int nblocks = (index + len) / valuesPerBlock - blockIndex;
    op.encode(arr, off, blocks, blockIndex, nblocks);
    final int diff = nblocks * valuesPerBlock;
    index += diff; len -= diff;

    if (index > originalIndex) {
      // stay at the block boundary
      return index - originalIndex;
    } else {
      // no progress so far => already at a block boundary but no full block to
      // set
      assert index == originalIndex;
      return super.set(index, arr, off, len);
    }
  }
   */
    // No direct access to the underlying long[] in AtomicIntArray, so we must fall back to the slow loop-based fill
//  public void fill(int fromIndex, int toIndex, long val) {
  @Override
  protected PackedInts.Format getFormat() {
    return PackedInts.Format.PACKED_SINGLE_BLOCK;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(bitsPerValue=" + bitsPerValue
        + ", size=" + size() + ", elements.length=" + blocks.length() + ")";
  }

  public static PackedOpportunistic create(DataInput in, int valueCount, int bitsPerValue) throws IOException {
    PackedOpportunistic reader = create(valueCount, bitsPerValue);
    for (int i = 0; i < reader.blocks.length(); ++i) {
      reader.set(i,  in.readLong());
    }
    return reader;
  }

  public static PackedOpportunistic create(int valueCount, int bitsPerValue) {
    switch (bitsPerValue) {
      case 1:
        return new PackedOpportunistic1(valueCount);
      case 2:
        return new PackedOpportunistic2(valueCount);
      case 3:
        return new PackedOpportunistic3(valueCount);
      case 4:
        return new PackedOpportunistic4(valueCount);
      case 5:
        return new PackedOpportunistic5(valueCount);
      case 6:
        return new PackedOpportunistic6(valueCount);
      case 7:
        return new PackedOpportunistic7(valueCount);
      case 8:
        return new PackedOpportunistic8(valueCount);
      case 9:
        return new PackedOpportunistic9(valueCount);
      case 10:
        return new PackedOpportunistic10(valueCount);
      case 12:
        return new PackedOpportunistic12(valueCount);
      case 16:
        return new PackedOpportunistic16(valueCount);
      case 21:
        return new PackedOpportunistic21(valueCount);
      case 32:
        return new PackedOpportunistic32(valueCount);
      default:
        throw new IllegalArgumentException("Unsupported number of bits per value: " + 32);
    }
  }

  static class PackedOpportunistic1 extends PackedOpportunistic {

    PackedOpportunistic1(int valueCount) {
      super(valueCount, 1);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 6;
      final int shift = index & 63; // b
//      final int shift = b << 0;
      return (blocks.get(o) >>> shift) & 1L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 6;
      final int shift = index & 63; // b
//      final int shift = b << 0;
      //blocks[o] = (blocks[o] & ~(1L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(1L << shift)) | (value << shift))) {
          break;
        }
        // Wait a bit to increase chances of non-collision
        // See http://java.dzone.com/articles/wanna-get-faster-wait-bit
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index >>> 6;
      final int shift = index & 63; // b
//      final int shift = b << 0;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 1L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(1L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic2 extends PackedOpportunistic {

    PackedOpportunistic2(int valueCount) {
      super(valueCount, 2);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 5;
      final int b = index & 31;
      final int shift = b << 1;
      return (blocks.get(o) >>> shift) & 3L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 5;
      final int b = index & 31;
      final int shift = b << 1;
//      blocks[o] = (blocks[o] & ~(3L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(3L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index >>> 5;
      final int b = index & 31;
      final int shift = b << 1;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 3L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(3L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic3 extends PackedOpportunistic {

    PackedOpportunistic3(int valueCount) {
      super(valueCount, 3);
    }

    @Override
    public long get(int index) {
      final int o = index / 21;
      final int b = index % 21;
      final int shift = b * 3;
      return (blocks.get(o) >>> shift) & 7L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 21;
      final int b = index % 21;
      final int shift = b * 3;
//      blocks[o] = (blocks[o] & ~(7L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(7L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index / 21;
      final int b = index % 21;
      final int shift = b * 3;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 7L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(7L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic4 extends PackedOpportunistic {

    PackedOpportunistic4(int valueCount) {
      super(valueCount, 4);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 4;
      final int b = index & 15;
      final int shift = b << 2;
      return (blocks.get(o) >>> shift) & 15L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 4;
      final int b = index & 15;
      final int shift = b << 2;
//      blocks[o] = (blocks[o] & ~(15L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(15L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index >>> 4;
      final int b = index & 15;
      final int shift = b << 2;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 15L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(15L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic5 extends PackedOpportunistic {

    PackedOpportunistic5(int valueCount) {
      super(valueCount, 5);
    }

    @Override
    public long get(int index) {
      final int o = index / 12;
      final int b = index % 12;
      final int shift = b * 5;
      return (blocks.get(o) >>> shift) & 31L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 12;
      final int b = index % 12;
      final int shift = b * 5;
//      blocks[o] = (blocks[o] & ~(31L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(31L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index / 12;
      final int b = index % 12;
      final int shift = b * 5;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 31L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(31L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic6 extends PackedOpportunistic {

    PackedOpportunistic6(int valueCount) {
      super(valueCount, 6);
    }

    @Override
    public long get(int index) {
      final int o = index / 10;
      final int b = index % 10;
      final int shift = b * 6;
      return (blocks.get(o) >>> shift) & 63L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 10;
      final int b = index % 10;
      final int shift = b * 6;
//      blocks[o] = (blocks[o] & ~(63L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(63L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index / 10;
      final int b = index % 10;
      final int shift = b * 6;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 63L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(63L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic7 extends PackedOpportunistic {

    PackedOpportunistic7(int valueCount) {
      super(valueCount, 7);
    }

    @Override
    public long get(int index) {
      final int o = index / 9;
      final int b = index % 9;
      final int shift = b * 7;
      return (blocks.get(o) >>> shift) & 127L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 9;
      final int b = index % 9;
      final int shift = b * 7;
//      blocks[o] = (blocks[o] & ~(127L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(127L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index / 9;
      final int b = index % 9;
      final int shift = b * 7;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 127L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(127L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic8 extends PackedOpportunistic {

    PackedOpportunistic8(int valueCount) {
      super(valueCount, 8);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 3;
      final int b = index & 7;
      final int shift = b << 3;
      return (blocks.get(o) >>> shift) & 255L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 3;
      final int b = index & 7;
      final int shift = b << 3;
//      blocks[o] = (blocks[o] & ~(255L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(255L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index >>> 3;
      final int b = index & 7;
      final int shift = b << 3;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 255L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(255L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic9 extends PackedOpportunistic {

    PackedOpportunistic9(int valueCount) {
      super(valueCount, 9);
    }

    @Override
    public long get(int index) {
      final int o = index / 7;
      final int b = index % 7;
      final int shift = b * 9;
      return (blocks.get(o) >>> shift) & 511L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 7;
      final int b = index % 7;
      final int shift = b * 9;
//      blocks[o] = (blocks[o] & ~(511L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(511L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index / 7;
      final int b = index % 7;
      final int shift = b * 9;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 511L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(511L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic10 extends PackedOpportunistic {

    PackedOpportunistic10(int valueCount) {
      super(valueCount, 10);
    }

    @Override
    public long get(int index) {
      final int o = index / 6;
      final int b = index % 6;
      final int shift = b * 10;
      return (blocks.get(o) >>> shift) & 1023L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 6;
      final int b = index % 6;
      final int shift = b * 10;
//      blocks[o] = (blocks[o] & ~(1023L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(1023L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index / 6;
      final int b = index % 6;
      final int shift = b * 10;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 1023L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(1023L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic12 extends PackedOpportunistic {

    PackedOpportunistic12(int valueCount) {
      super(valueCount, 12);
    }

    @Override
    public long get(int index) {
      final int o = index / 5;
      final int b = index % 5;
      final int shift = b * 12;
      return (blocks.get(o) >>> shift) & 4095L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 5;
      final int b = index % 5;
      final int shift = b * 12;
//      blocks[o] = (blocks[o] & ~(4095L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(4095L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index / 5;
      final int b = index % 5;
      final int shift = b * 12;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 4095L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(4095L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic16 extends PackedOpportunistic {

    PackedOpportunistic16(int valueCount) {
      super(valueCount, 16);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 2;
      final int b = index & 3;
      final int shift = b << 4;
      return (blocks.get(o) >>> shift) & 65535L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 2;
      final int b = index & 3;
      final int shift = b << 4;
//      blocks[o] = (blocks[o] & ~(65535L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(65535L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index >>> 2;
      final int b = index & 3;
      final int shift = b << 4;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 65535L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(65535L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic21 extends PackedOpportunistic {

    PackedOpportunistic21(int valueCount) {
      super(valueCount, 21);
    }

    @Override
    public long get(int index) {
      final int o = index / 3;
      final int b = index % 3;
      final int shift = b * 21;
      return (blocks.get(o) >>> shift) & 2097151L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index / 3;
      final int b = index % 3;
      final int shift = b * 21;
//      blocks[o] = (blocks[o] & ~(2097151L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(2097151L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index / 3;
      final int b = index % 3;
      final int shift = b * 21;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 2097151L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(2097151L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

  static class PackedOpportunistic32 extends PackedOpportunistic {

    PackedOpportunistic32(int valueCount) {
      super(valueCount, 32);
    }

    @Override
    public long get(int index) {
      final int o = index >>> 1;
      final int b = index & 1;
      final int shift = b << 5;
      return (blocks.get(o) >>> shift) & 4294967295L;
    }

    @Override
    public void set(int index, long value) {
      final int o = index >>> 1;
      final int b = index & 1;
      final int shift = b << 5;
//      blocks[o] = (blocks[o] & ~(4294967295L << shift)) | (value << shift);
      while (true) {
        final long old = blocks.get(o);
        if (blocks.compareAndSet(o, old, (old & ~(4294967295L << shift)) | (value << shift))) {
          break;
        }
        LockSupport.parkNanos(1);
      }
    }

    @Override
    public long inc(int index) {
      final int o = index >>> 1;
      final int b = index & 1;
      final int shift = b << 5;
      while (true) {
        final long old = blocks.get(o);
        final long newValue = ((old >>> shift) & 4294967295L)+1;
        final long setNew = newValue == incOverflow ? 0 : newValue << shift;
        if (blocks.compareAndSet(o, old, (old & ~(4294967295L << shift)) | setNew)) {
          return newValue;
        }
        LockSupport.parkNanos(1);
      }
    }
  }

}