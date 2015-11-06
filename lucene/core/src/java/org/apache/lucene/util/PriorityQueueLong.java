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

import java.util.Iterator;

/**
 * Priority Queue for longs. The queue represents the long-entries collapsed into a single long[],
 * which ensures near-zero memory overhead if the queue is full, compared to an array of arrays.
 * This also helps with memory access locality, although the basic scattered memory access problem
 * of using heaps still remains.
 * </p><p>
 * Intended use for this implementation is to act as a backing queue for Java Objects that can be mapped
 * to and from long without too much overhead.
 * </p><p>
 * Note: This heap is lazy: Until the heap is full, inserts are done without ordering the heap.
 * </p><p>
 * Besides the abstract methods, it is <em>highly</em> recommended to override and optimize
 * {@link #lessThan(long, long)} and {@link #lessThan(Object, long)} as those methods are
 * used extensively at the core of most heap operations.
 **/
public abstract class PriorityQueueLong<T> {
  protected final int maxSize;
  protected final long[] elements;

  protected int size = 0;
  protected boolean dirty = false;

  /**
   * @param elementCount The maximum number of elements.
   */
  public PriorityQueueLong(int elementCount){
    maxSize = elementCount+1;
    elements = new long[maxSize];
    clear();
  }

  /**
   * Insert a new element into the queue, removing the lowest existing element if the queue is full.
   * @param element the element to insert. This object can be safely re-used by the caller.
   * @return null if the queue is not qet full, else the previously lowest element. This object is not independent
   * of the queue and must not be modified by the caller.
   */
  public T insert(T element) {
    if (size < maxSize-1) {
      elements[++size] = serialize(element);
      dirty = true;
      return null;
    } else if (size > 0 && !lessThan(element, elements[1])) {
      orderHeap();
      elements[1] = serialize(element);
      downHeap();
      return deserialize(elements[1], element);
    } else {
      return element;
    }
  }

  // Important: reuse can be null!
  protected abstract T deserialize(long block, T reuse);

  protected abstract long serialize(T element);

  protected void downHeap() {
    int i = 1;
    final long downHeapOld = elements[i]; // save top node
    int j = i << 1;            // find smaller child
    int k = j + 1;
    if (k <= size && lessThan(k, j)) {
      j = k;
    }
    while (j <= size && lessThan(j, downHeapOld)) {
      elements[i] = elements[j]; // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && lessThan(k, j)) {
        j = k;
      }
    }
    elements[i] = downHeapOld;
  }

  public final void clear() {
    size = 0;
    dirty = false;
  }

  public int size() {
    return size;
  }

  public int capacity() {
    return maxSize-1;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public Iterator<T> getFlushingIterator(boolean unordered, boolean reuseElement) {
    if (!unordered && dirty) { // Order needed, but no previous order
      orderHeap(); // This could be replaced with a mergesort that handles elementSize
    }
    return unordered ? new DirectIterator(reuseElement) : new HeapIterator(reuseElement);
  }

  public final T top(T reuse) {
    if (size() == 0) {
      return null;
    }
    orderHeap();
    return deserialize(elements[1], reuse);
  }

  /** Removes and returns the least element of the PriorityQueue in log(size) time. */
  public final T pop(T reuse) {
    if (size == 0) {
      return null;
    }
    orderHeap();
    reuse = deserialize(elements[1], reuse); // save first value
    elements[1] = elements[size--];  // move last to first
    downHeap();               // adjust heap
    return reuse;
  }

  protected void orderHeap() {
    if (!dirty) {
      return;
    }
    heapSort(1, size-1);
    dirty = false;
  }

  private void heapSort(int from, int to) {
    if (to - from <= 1) {
      return;
    }
    heapify(from, to);
    for (int end = to - 1; end > from; --end) {
      swap(from, end);
      siftDown(from, from, end);
    }
  }

  private void heapify(int from, int to) {
    for (int i = heapParent(from, to - 1); i >= from; --i) {
      siftDown(i, from, to);
    }
  }

  private void siftDown(int i, int from, int to) {
    for (int leftChild = heapChild(from, i); leftChild < to; leftChild = heapChild(from, i)) {
      final int rightChild = leftChild + 1;
      if (lessThan(i, leftChild)) {
        if (rightChild < to && lessThan(leftChild, rightChild)) {
          swap(i, rightChild);
          i = rightChild;
        } else {
          swap(i, leftChild);
          i = leftChild;
        }
      } else if (rightChild < to && lessThan(i, rightChild)) {
        swap(i, rightChild);
        i = rightChild;
      } else {
        break;
      }
    }
  }

  private int heapParent(int from, int i) {
    return ((i - 1 - from) >>> 1) + from;
  }
  private int heapChild(int from, int i) {
    return ((i - from) << 1) + 1 + from;
  }

  private void swap(int indexA, int indexB) {
    long temp = elements[indexA];
    elements[indexA] = elements[indexB];
    elements[indexB] = temp;
  }

  public abstract boolean lessThan(T elementA, T elementB);

  /**
   * It is highly recommended to override this method, if a fast comparison of serialized elements can be implemented.
   */
  public boolean lessThan(long elementA, long elementB) {
    return lessThan(deserialize(elementA, null), deserialize(elementB, null));
  }

  protected boolean lessThan(T element, long block) {
    return lessThan(serialize(element), block);
  }

  // Assumes the heap is ordered
  private class HeapIterator implements Iterator<T> {
    private final T iteratorCache;
    public HeapIterator(boolean reuseElement) {
      iteratorCache = reuseElement ? deserialize(elements[0], null) : null;
    }

    @Override
    public boolean hasNext() {
      return !isEmpty();
    }

    @Override
    public T next() {
      return pop(iteratorCache);
    }
  }

  // Delivers the values in the array order
  private class DirectIterator implements Iterator<T> {
    private final T iteratorCache;
    private int index = 1;
    public DirectIterator(boolean reuseElement) {
      iteratorCache = reuseElement ? deserialize(elements[0], null) : null;
    }

    @Override
    public boolean hasNext() {
      if (index > size) {
        clear();
        return false;
      }
      return true;
    }

    @Override
    public T next() {
      return deserialize(elements[index++], iteratorCache);
    }
  }

}
