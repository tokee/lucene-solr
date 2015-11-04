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
 * Priority Queue for int arrays. The queue represents the int[]-entries collapsed into a single int[],
 * which ensures very little memory overhead, compared to an array of arrays. This also helps with
 * memory access locality, although the basic scattered memory access problem of using heaps still remains.
 * </p><p>
 * Intended use for this implementation is to act as a backing queue for Java Objects that can be mapped
 * to and from int[] without too much overhead.
 * </p><p>
 * Note: This heap is lazy: Until the heap is full, inserts are done without ordering the heap.
 * </p><p>
 * Besides the abstract methods, it is <em>highky</em> recommended to override and optimize
 * {@link #lessThan(int[], int, int[], int)} and {@link #lessThan(Object, int[], int)} as those methods are
 * used extensively at the core of most heap operations.
 **/
public abstract class PriorityQueueInts<T> {
  protected final int maxSize;
  protected final int elementSize;
  protected final int[] elements;

  protected int size = 0;
  protected boolean dirty = false;

  private final int[] insertCache;
  private final int[] downHeapCache;
  private final int[] swapAndLessThanCache;

  /**
   * @param elementCount The maximum number of elements.
   * @param elementSize  The length of each element ({@code int[]}).
   */
  public PriorityQueueInts(int elementCount, int elementSize){
    maxSize = elementCount+1; // Our heap starts at 1 as that uses simpler math to calculate parents and children nodes
    this.elementSize = elementSize;
    elements = new int[(elementCount+1)*elementSize];
    if (((long)elementCount+1)*((long)elementSize) > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("The number (" + elementCount + ") and size (" + elementSize + ") of " +
          "elements for this queue cannot be represented in a single Java int[]");
    }
    insertCache = new int[elementSize];
    downHeapCache = new int[elementSize];
    swapAndLessThanCache = new int[elementSize];
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
      serialize(element, elements, ++size*elementSize);
      dirty = true;
      return null;
    } else if (size > 0 && !lessThan(element, elements, elementSize)) {
      orderHeap();
      serialize(element, elements, 1);
      downHeap();
      return deserialize(elements, elementSize, element);
    } else {
      return element;
    }
  }

  // Important: reuse can be null!
  protected abstract T deserialize(int[] ints, int offset, T reuse);

  protected abstract void serialize(T element, int[] ints, int offset);

  private void downHeap() {
    int i = 1;
    assign(i, downHeapCache); // save top node
    int j = i << 1;           // find smaller child
    int k = j + 1;
    if (k <= size && lessThan(k, j)) {
      j = k;
    }
    while (j <= size && lessThan(j, downHeapCache)) {
      assign(j, i);            // shift up child
      i = j;
      j = i << 1;
      k = j + 1;
      if (k <= size && lessThan(k, j)) {
        j = k;
      }
    }
    assign(downHeapCache, i);
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
    return deserialize(elements, elementSize, reuse);
  }

  /** Removes and returns the least element of the PriorityQueue in log(size) time. */
  public final T pop(T reuse) {
    if (size == 0) {
      return null;
    }
    orderHeap();
    reuse = deserialize(elements, elementSize, reuse); // save first value
    assign(size, 1);          // move last to first
    size--;
    downHeap();               // adjust heap
    return reuse;
  }

  private void orderHeap() {
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

  private void assign(int fromIndex, int toIndex) {
    System.arraycopy(elements, fromIndex*elementSize, elements, toIndex*elementSize, elementSize);
  }
  private void swap(int indexA, int indexB) {
    assign(indexA, swapAndLessThanCache);
    assign(indexB, indexA);
    assign(swapAndLessThanCache, indexB);
  }
  private int[] assign(int fromIndex, int[] toElement) {
    System.arraycopy(elements, fromIndex * elementSize, toElement, 0, elementSize);
    return toElement;
  }
  private void assign(int[] fromElement, int toIndex) {
    System.arraycopy(fromElement, 0, elements, toIndex*elementSize, elementSize);
  }
  private int[] assign(int[] fromElement, int[] toElement) {
    System.arraycopy(fromElement, 0, toElement, 0, elementSize);
    return toElement;
  }

  public abstract boolean lessThan(T elementA, T elementB);

  /**
   * It is highly recommended to override this method, if a fast comparison of serialized elements can be implemented.
   * @param elementA Array containing the first serialized element, starting at offsetA.
   * @param offsetA  The start position of the first element.
   * @param elementB Array containing the second serialized element, starting at offsetB.
   * @param offsetB  The start position of the second element.
   * @return -1 if elementA < elementB, 1 if elementB > elementA, 0 if elementA == elementB.
   */
  public boolean lessThan(int[] elementA, int offsetA, int[] elementB, int offsetB) {
    return lessThan(deserialize(elementA, offsetA, null), deserialize(elementB, offsetB, null));
  }

  protected boolean lessThan(T element, int[] ints, int offset) {
    serialize(element, swapAndLessThanCache, 0);
    return lessThan(swapAndLessThanCache, 0, ints, offset);
  }

  protected final boolean lessThan(int index, int[] element) {
    return lessThan(elements, index*elementSize, element, 0);
  }

  protected final boolean lessThan(int indexA, int indexB) {
    return lessThan(elements, indexA*elementSize, elements, indexB*elementSize);
  }

  // Assumes the heap is ordered
  private class HeapIterator implements Iterator<T> {
    private final T iteratorCache;
    public HeapIterator(boolean reuseElement) {
      iteratorCache = reuseElement ? deserialize(elements, elementSize, null) : null;
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
      iteratorCache = reuseElement ? deserialize(elements, elementSize, null) : null;
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
      return deserialize(elements, index++ * elementSize, iteratorCache);
    }
  }

}
