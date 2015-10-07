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

import java.util.Locale;

/**
 * BHeap: http://playfulprogramming.blogspot.it/2015/08/cache-optimizing-priority-queue.html
 **/
public class BHeap {
  public static final long SENTINEL = Long.MAX_VALUE; // Must be pre-filled with sentinels! TODO: Make sentinel 0?

  private final int maxSize;
  final long[] elements;
  private int mhIndex = 1;
  private int mhOffset = 1;
  private int size = 0;

  final int MH_EXP; // 2^4-1 = 15 longs in each mini-heap
  final int MH_MAX; // 2^4-1 = 15 longs in each mini-heap


  /**
   * Creates a queue for ScoreDocs.
   * @param size maximum size of the queue.
   */
  public BHeap(int size, int miniheapExponent){
    maxSize = size;
    MH_EXP = miniheapExponent;
    MH_MAX = (1 << MH_EXP)-1;
    elements = new long[(maxSize/MH_MAX+3)<<MH_EXP]; // 1 wasted entry/miniheap, 1 wasted miniheap/heap
    clear();
  }

  public long insert(long element) {
    if (size < maxSize) {
      set(mhIndex, mhOffset, element);
      orderUp(mhIndex, mhOffset);
      mhOffset++;
      if (mhOffset > MH_MAX) {
        mhIndex++;
      }
      size++;
      return SENTINEL;
    } else if (size > 0 && element < top()) {
      long oldElement = top();
      set(1, 1, element);
      orderDown(1, 1);
      return oldElement;
    } else {
      return element;
    }
  }

  /**
   * Orders the heap of miniheaps downwards, where the miniheaps are assumed to be ordered,
   * except for the element at mhOffset in the miniheap at mhIndex.
   * @param mhIndex  the index for the miniheap containing an out-of-order element.
   * @param mhOffset the element that is assumed to be out of order.
   */
  private void orderDown(int mhIndex, int mhOffset) {
    long element = get(mhIndex, mhOffset);
    while (mhIndex < elements.length >> MH_EXP &&
        (mhOffset = orderDownMH(mhIndex, mhOffset) & MH_EXP) != 0) { // element at bottom of miniheap
      int mhChildAIndex = mhIndex << 1;
      int mhChildBIndex = mhChildAIndex+1;
      if (mhChildBIndex < elements.length >> MH_EXP && get(mhChildBIndex, 1) < get(mhChildAIndex, 1)) {
        mhChildAIndex = mhChildBIndex;
      }
      long elementBelow = get(mhChildAIndex, 1);
      if (element <= elementBelow) { // element <= least element in miniheap below
        break;
      }
      // Swap and switch to miniheap below
      set(mhIndex, mhOffset, elementBelow);
      set(mhChildAIndex, 1, element);
      mhIndex = mhChildAIndex;
      mhOffset = 1;
    }
  }

  /**
   * Orders the miniheap downwards, where the miniheap is assumed to be ordered,
   * except for the element at mhOffset in the miniheap at mhIndex.
   * @param mhIndex  the index for the miniheap containing an out-of-order element.
   * @param mhOffset the element that is assumed to be out of order.
   * @return the offset for the position in the miniheap where the unordered element ended.
   */
  private int orderDownMH(int mhIndex, int mhOffset) {
    long oldElement = get(mhIndex, mhOffset);
    int childA = mhOffset << 1;            // find smaller child
    int ChildB = childA + 1;
    if (ChildB <= size && get(mhIndex, ChildB) < get(mhIndex, childA)) {
      childA = ChildB;
    }
    while (childA <= MH_MAX && get(mhIndex, childA) < oldElement) {
      set(mhIndex, mhOffset, get(mhIndex, childA));            // shift up child
      mhOffset = childA;
      childA = mhOffset << 1;
      ChildB = childA + 1;
      if (ChildB <= MH_MAX && get(mhIndex, ChildB) < get(mhIndex, childA)) {
        childA = ChildB;
      }
    }
    set(mhIndex, mhOffset, oldElement);
    return mhOffset;
  }

  private long top() {
    return get(1, 1);
  }

  /**
   * Orders the heap of miniheaps upwards, where the miniheaps are assumed to be ordered,
   * except for the element at mhOffset in the miniheap at mhIndex.
   * @param mhIndex  the index for the miniheap containing an out-of-order element.
   * @param mhOffset the element that is assumed to be out of order. This must be at the bottom of the miniheap.
   */
  private void orderUp(int mhIndex, int mhOffset) {
    final long newElement = get(mhIndex, mhOffset);
    while (orderUpMH(mhIndex, mhOffset) != 1) { // Ordering the miniheap caused the top to change
      int mhParentIndex = mhIndex >> 1;
      if (mhParentIndex == 0) {
        break; // top reached
      }
      // (1 << (MH_EXP-1)) == bottom row origo in the paren miniheap
      // mhIndex - (mhParentIndex << 1) == Offset in the bottom row of the parent miniheap
      int mhParentOffset = (1 << (MH_EXP-1)) + mhIndex - (mhParentIndex << 1);
      if (get(mhParentIndex, mhParentOffset) < newElement) {
        break; // Do not bubble further up
      }
      // Swap ant bubble up in the parent miniheap
      set(mhIndex, 1, get(mhParentIndex, mhParentOffset));
      set(mhParentIndex, mhParentOffset, newElement);
      mhIndex = mhParentIndex;
      mhOffset = mhParentOffset;
    }
  }

  /**
   * Orders a miniheap where the miniheap is assumed to be ordered, except for the element at mhOffset.
   * @param mhIndex  the index for the miniheap.
   * @param mhOffset the element that is assumed to be out of order. This must be at the bottom of the miniheap.
   * @return the mhOffset of where the element came to a rest
   */
  private int orderUpMH(int mhIndex, int mhOffset) {
    final long problemChild = get(mhIndex, mhOffset);
    int parent = mhOffset >>> 1;
    while (parent > 0 && problemChild < get(mhIndex, parent)) {
      set(mhIndex, mhOffset, get(mhIndex, parent)); // shift parent down
      mhOffset = parent;
      parent = mhOffset >>> 1;
    }
    set(mhIndex, mhOffset, problemChild);
    return mhOffset;
  }

  long get(int mhIndex, int mhOffset) {
    return elements[(mhIndex << MH_EXP) + mhOffset];
  }

  void set(int mhIndex, int mhOffset, long element) {
    elements[(mhIndex << MH_EXP) + mhOffset] = element;
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
    mhIndex = 1;
    mhOffset = 1;
  }

  public String toString(boolean verbose) {
    if (!verbose) {
      return "BHeap(" + size + "/" + maxSize + ")";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("BHeap size=").append(size).append("\n");
    for (int mhIndex = 1 ; mhIndex < elements.length >> MH_EXP ; mhIndex++) {
      sb.append(String.format(Locale.ENGLISH, "miniheap %2d: ", mhIndex));
      for (int mhOffset = 1 ; mhOffset <= MH_MAX ; mhOffset++) {
        if ((mhIndex << MH_EXP) + mhOffset >= elements.length) {
          break;
        }
        if (mhOffset > 1) {
          sb.append(", ");
        }
        sb.append(Long.toString(get(mhIndex, mhOffset)));
      }
      sb.append("\n");
    }
    return sb.toString();
  }
}
