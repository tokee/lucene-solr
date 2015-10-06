package org.apache.lucene.search;

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

import java.util.Arrays;
import java.util.Iterator;

/**
 * BHeap: http://playfulprogramming.blogspot.it/2015/08/cache-optimizing-priority-queue.html
 *
 **/
public class BHeap {
  private final int maxSize;
  private final long[] elements;
  private int mhIndex = 0;
  private int mhOffset = 1;
  private int size = 0;

  private final int MH_EXP = 4; // 2^4-1 = 15 longs in each mini-heap
  private final int MH_MAX = (1 << MH_EXP)-1; // 2^4-1 = 15 longs in each mini-heap


  /**
   * Creates a queue for ScoreDocs.
   * @param size maximum size of the queue.
   */
  public BHeap(int size){
    maxSize = size;
    elements = new long[maxSize]; //    /7*8?
    clear();
  }

  public void insert(long element) {
    if (maxSize == 0 || (size == maxSize && element < elements[1])) {
      return;
    }
    if (size < maxSize) {
      assignAndSiftUp(element);
      size++;
      if (++mhOffset > MH_MAX) {
        mhIndex++;
        mhOffset = 1;
      }
    } else {
      elements[1] = element;
      siftDown(0, 1);
    }
  }

  private void assignAndSiftUp(final long newElement) {
    final int mhOrigo = mhIndex << MH_EXP;
    elements[(mhOrigo + mhOffset] = newElement;
    int offset = mhOffset;
    int parent = offset >> 1;
    while (parent > 0 && newElement < elements[parent] ) {
      elements[mhOrigo + offset] = elements[mhOrigo + parent]; // shift parents down
      offset = parent;
      parent = offset >> 1;
    }
    if (parent == 0) { // Top of mini-heap. Find mhParent and check if the value is smaller

    }

    if (offset == 1) {
      if (mhStart == 1) {
        return; // Reached the top
      }
      final int mhParent = mhStart >>> 1;

    }
  }


  private void clear() {
    size = 0;
  }

}
