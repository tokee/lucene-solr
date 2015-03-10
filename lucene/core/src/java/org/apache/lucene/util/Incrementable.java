package org.apache.lucene.util;

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

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

import java.io.IOException;

/**
 * Allows for increments (add 1) to the underlying structure.
 * </p><p>
 * This is a temporary interface as its method should be added to PackedInts.Mutable.
 * Relative adjustments of values are used for counter structures and similar.
 * As the implementations of PackedInts.Mutable tend to use complicated logic to
 * access the bits for the values, avoiding the standard get-set dul calls and
 * using a single call will have the same or better performance.
 * // TODO: Consider a method that takes a delta instead
 */
public interface Incrementable {
  /**
   * Increment the value at the given index by 1.
   * @param index the index for the value to increment.
   */
  public void inc(int index);

  /**
   * Extremely simple wrapper for easy construction of an Incrementable mutable.
   */
  public static class IncrementableMutable extends PackedInts.Mutable implements Incrementable {
    private final PackedInts.Mutable backend;

    // This implementation should be in PackedInts.MutableImpl
    @Override
    public void inc(int index) {
      backend.set(index, backend.get(index)+1);
    }

    // Direct delegates below

    public IncrementableMutable(PackedInts.Mutable backend) {
      this.backend = backend;
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
    public Object getArray() {
      return backend.getArray();
    }

    @Override
    public boolean hasArray() {
      return backend.hasArray();
    }

    @Override
    public long get(int docID) {
      return backend.get(docID);
    }
  }


}
