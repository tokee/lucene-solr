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

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

/**
 * Allows for increments (add 1) to the underlying structure.
 * </p><p>
 * Relative adjustments of values are used for counter structures and similar.
 * As the implementations of PackedInts.Mutable tend to use complicated logic to
 * access the bits for the values, replacing the standard get-set calls with a
 * single call will have the same or better performance.
 * // TODO: Consider a method that takes a delta instead, to make it more general
 */
public interface Incrementable {
  /**
   * Increment the value at the given index by 1.
   * If the value overflows, 0 must be stored as the value at index.
   * @param index the index for the value to increment.
   */
  void increment(int index);

  /**
   * Increment the value at the given index by 1 and return the overall status of the operation.
   * If the value overflows, 0 must be stored as the value at the index.
   *
   * This operation is used to build efficient tracking structures, where it is important to
   * know whether this was the first increment call (wasZero) or not.
   * @param index the index for the value to increment.
   * @return the result of the operation.
   */
  STATUS incrementStatus(int index);
  enum STATUS { // All are mutually exclusive as counters always goes to > 0
    // TODO: Add blockWasZero if it really saves time (nplanez is candidate for time win)
//    blockWasZero, // The whole logical block (normally 64 counters) was zero before increment. Optional
    wasZero,      // The counter was zero before increment
    ok,           // Increment from non-zero without overflow
    overflowed    // Overflow occurred due to the increment
  }

  /*
   * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
   *This method is optional. Implementations must signal support with {@link #hasCompareAndSet()}.
   * @param index the index for the value to set.
   * @param expect the expected value
   * @param update the new value
   * @return true if successful. False return indicates that
   * the actual value was not equal to the expected value.
   */
  boolean compareAndSet(int index, long expect, long update);
  boolean hasCompareAndSet();


  /**
   * Simple wrapper for easy construction of an Incrementable mutable.
   */
  class IncrementableMutable extends PackedInts.Mutable implements Incrementable {
    private final PackedInts.Mutable backend;
    private final long incOverflow;

    // This implementation should be in PackedInts.MutableImpl
    // Note the guard against overflow
    @Override
    public void increment(int index) {
      final long value = backend.get(index)+1;
      backend.set(index, value == incOverflow ? 0 : value);
    }

    @Override
    public STATUS incrementStatus(int index) {
      final long value = backend.get(index)+1;
      backend.set(index, value == incOverflow ? 0 : value);
      return value == 1 ? STATUS.wasZero : value == incOverflow ? STATUS.overflowed : STATUS.ok;
    }

    @Override
    public boolean compareAndSet(int index, long expect, long update) {
      if (get(index) == expect) {
        set(index, update);
        return true;
      }
      return false;
    }

    @Override
    public boolean hasCompareAndSet() {
      return true;
    }

    // Direct delegates below

    public IncrementableMutable(PackedInts.Mutable backend) {
      this.backend = backend;
      this.incOverflow = (long) Math.pow(2, backend.getBitsPerValue());
    }

    @Override
    public void set(int index, long value) {
      backend.set(index, value);
    }

    @Override
    public int set(int index, long[] arr, int off, int len) {
      return backend.set(index, arr, off, len);
    }

    @Override
    public void fill(int fromIndex, int toIndex, long val) {
      backend.fill(fromIndex, toIndex, val);
    }

    @Override
    public void clear() {
      backend.clear();
    }

    @Override
    public void save(DataOutput out) throws IOException {
      backend.save(out);
    }

    @Override
    public int get(int index, long[] arr, int off, int len) {
      return backend.get(index, arr, off, len);
    }

    @Override
    public int getBitsPerValue() {
      return backend.getBitsPerValue();
    }

    @Override
    public int size() {
      return backend.size();
    }

    @Override
    public long ramBytesUsed() {
      return backend.ramBytesUsed();
    }

    @Override
    public long get(int docID) {
      return backend.get(docID);
    }
  }


}
