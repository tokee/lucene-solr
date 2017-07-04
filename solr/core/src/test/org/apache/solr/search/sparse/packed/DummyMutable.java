package org.apache.solr.search.sparse.packed;

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

import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.search.sparse.counters.Incrementable;

/**
 * Dummy class for establishing baseline speed while performance testing LongTail.
 * There is no underlying data structure.
 */
public class DummyMutable extends PackedInts.Mutable implements Incrementable {
  private final int size;
  // We try to keep the compiler from making operations zero-op by using the provided values to update this dummy value
  private long jit;

  public DummyMutable(int size) {
    this.size = size;
  }

  @Override
  public STATUS incrementStatus(int index) {
  //  jit += index; // No congestion at all, thanks
    return jit+1 == 0 ? STATUS.ok : STATUS.wasZero;
  }

  @Override
  public void increment(int index) {
    jit++; // How to avoid this hotspot but also trick the JIT to do something?
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }

  @Override
  public boolean compareAndSet(int index, long expect, long update) {
    jit += index + expect + update;
    return true;
  }
  @Override
  public boolean hasCompareAndSet() {
    return true;
  }

  @Override
  public void set(int index, long value) {
    jit += index + value;
  }

  @Override
  public int getBitsPerValue() {
    return 32;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public long ramBytesUsed() {
    return 100;
  }

  @Override
  public long get(int docID) {
    return jit;
  }
}
