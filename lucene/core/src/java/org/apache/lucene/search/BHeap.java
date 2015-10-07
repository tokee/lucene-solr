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

/**
 * BHeap: http://playfulprogramming.blogspot.it/2015/08/cache-optimizing-priority-queue.html
 **/
public class BHeap {
  public static final long SENTINEL = Long.MAX_VALUE; // Must be pre-filled with sentinels! TODO: Make sentinel 0?

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

  /**
   * Orders a miniheap where the miniheap is assumed to be ordered, except for the element at mhOffset.
   * @param mhIndex  the index for the miniheap.
   * @param mhOffset the element that is assumed to be out of order. This must be at the bottom of the miniheap.
   * @return the element at the top of the miniheap if that was changed (or if mhOffset pointed to it). Else SENTINEL.
   */
  private long orderUpMH(int mhIndex, int mhOffset) {
    final long problemChild = elements[abs(mhIndex, mhOffset)];
    int parent = mhOffset >>> 1;
    while (parent > 0 && problemChild < get(mhIndex, parent)) {
      set(mhIndex, mhOffset, get(mhIndex, parent)); // shift parent down
      mhOffset = parent;
      parent = mhOffset >>> 1;
    }
    return get(mhIndex, mhOffset); // If parent == 0, this will be sentinel
  }

  /**
   * Orders the heap of miniheaps, where the miniheaps are assumed to be ordered, except for the element at mhOffset
   * in the miniheap at mhIndex.
   * @param mhIndex  the index for the miniheap containing an out-of-order element.
   * @param mhOffset the element that is assumed to be out of order. This must be at the bottom of the miniheap.
   */
  private void orderUp(int mhIndex, int mhOffset) {
    long top;
    while ((top = orderUpMH(mhIndex, mhOffset)) != SENTINEL) { // Ordering the miniheap caused the top to change
      int mhParentIndex = mhIndex >> 1;
      if (mhParentIndex == 0) {
        break; // top reached
      }
      // (1 << (MH_EXP-1)) == bottom row origo in the paren miniheap
      // mhIndex - (mhParentIndex << 1) == Offset in the bottom row of the parent miniheap
      int mhParentOffset = (1 << (MH_EXP-1)) + mhIndex - (mhParentIndex << 1);
      if (get(mhParentIndex, mhParentOffset) < top) {
        break; // Do not bubble further up
      }
      // Swap ant bubble up in the parent miniheap
      set(mhIndex, 1, get(mhParentIndex, mhParentOffset));
      set(mhParentIndex, mhParentOffset, top);
      mhIndex = mhParentIndex;
      mhOffset = mhParentOffset;
    }
  }

  private int abs(int mhIndex, int mhOffset) {
    return mhIndex << MH_EXP + mhOffset;
  }

  private long get(int mhIndex, int mhOffset) {
    return elements[mhIndex << MH_EXP + mhOffset];
  }

  private void set(int mhIndex, int mhOffset, long element) {
    elements[mhIndex << MH_EXP + mhOffset] = element;
  }

  /*
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

    */
  private void clear() {
    size = 0;
  }

}
