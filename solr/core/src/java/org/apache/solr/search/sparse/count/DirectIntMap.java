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

package org.apache.solr.search.sparse.count;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

/**
 * An integer to integer map that uses a single long[] as backing storage.
 * Compactness and performance of the map relies on the lower bits of the
 * key-values being random.
 * </p><p>
 * Calling get for a non-defined key returns Integer.MAX_VALUE;
 * </p><p>
 * Putting Integer.MAX_VALUE, Integer.MAX_VALUE will result in undetermined
 * behaviour.
 * </p><p>
 * This implementation is not Thread-safe.
 */
public class DirectIntMap {
  private static final long VALUE_MASK = 0xffffffffL; // Lower 32 bits
  public static final int DEFAULT_INITIAL_SIZE = 128;
  public static final int DEFAULT_BUCKET_SIZE = 16;
  public static final double BUCKET_EXTEND_FACTOR = 1.5;

  private int capacity;
  private int size = 0;
  private int bucketSize;
  private int bucketBits;
  private int bucketMask;
  private long[] store;

  public DirectIntMap(int bucketBits, int bucketSize) {
    this.bucketBits = bucketBits;
    this.bucketMask = ~(~1 << bucketBits);
    this.bucketSize = bucketSize;
    capacity = (bucketMask+1)*bucketSize;
    store = new long[bucketSize*(bucketMask+1)];
    Arrays.fill(store, Long.MAX_VALUE);
  }

  public int get(int key) {
    final int bucketStart = (key & bucketMask)*bucketSize;
    final int bucketEnd = bucketStart + bucketSize;
    for (int pos = bucketStart ; pos < bucketEnd ; pos++) {
      if (store[pos] == Long.MAX_VALUE) {
        break;
      }
      if (store[pos] >>> 32 == key) {
        return (int) (store[pos] & VALUE_MASK);
      }
    }
    return Integer.MAX_VALUE;
  }

  public void put(int key, int value) {
    final int bucketStart = (key & bucketMask)*bucketSize;
    final int bucketEnd = bucketStart + bucketSize;
    for (int pos = bucketStart ; pos < bucketEnd ; pos++) {
      if (store[pos] >>> 32 == key || store[pos] == Long.MAX_VALUE) {
        store[pos] = (((long) key) << 32) | value;
        size++;
        return;
      }
    }
    // Not enough room: We need to extend
    // Determine if we should extend by re-hashing or extending buckets
    if (hashDoublingExtendsBucket(key)) {
      int newBucketBits = bucketBits + 1;
      DirectIntMap cim = new DirectIntMap(newBucketBits, bucketSize);
      cim.addAll(this);
      this.capacity = cim.capacity;
      this.size = cim.size;
      this.bucketBits = cim.bucketBits;
      this.bucketMask = cim.bucketMask;
      this.store = cim.store;
    } else { // Extend bucket size
      int newBucketSize = (int) (bucketSize * BUCKET_EXTEND_FACTOR);
      long[] newStore = new long[newBucketSize * (bucketMask + 1)];
      for (int bucket = 0; bucket < bucketMask + 1; bucket++) {
        System.arraycopy(store, bucket * bucketSize, newStore, bucket * newBucketSize, bucketSize);
      }
      store = newStore;
      bucketSize = newBucketSize;
    }
    put(key, value); // We know there's room now
  }

  private boolean hashDoublingExtendsBucket(int key) {
    final int bucketStart = (key & bucketMask)*bucketSize;
    final int bucketEnd = (key & bucketMask) + bucketSize;
    final int extendedMask = (bucketMask << 1) | 1;
    long lastMaskedKey = 0;
    for (int pos = bucketStart ; pos < bucketEnd ; pos++) {
      if (store[pos] == Long.MAX_VALUE) {
        throw new IllegalStateException("hashDoublingExtendsBucket should never be called when there is free room");
      }
      final long extendedKey = (store[pos] >> 32) & extendedMask;
      if (pos != 0 && extendedKey != lastMaskedKey) {
        return true;
      }
      lastMaskedKey = extendedKey;
    }
    return false; // Extended hashes are still equal
  }

  private void addAll(DirectIntMap compactIntMap) {
    for (long keyValuePair: compactIntMap.store) {
      if (keyValuePair != Long.MAX_VALUE) {
        put((int) (keyValuePair >>> 32), (int) (keyValuePair & VALUE_MASK));
      }
    }
  }

  public long ramBytesUsed() {
    return RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
            + 5 * RamUsageEstimator.NUM_BYTES_INT
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF) // store ref
        + RamUsageEstimator.sizeOf(store);
  }
}
