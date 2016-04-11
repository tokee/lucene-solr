package org.apache.lucene.search.grouping.term;

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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Based on the logic from {@link org.apache.lucene.search.grouping.AbstractFirstPassGroupingCollector}.
 * Maintains a top-N queue of pairs, where pair-keys are guaranteed to be unique and where values determine
 * the order.
 * @param <K> tke key type.
 * @param <V> the value type.
 * @param <P> the optional payload type.
 */
public class CollapsingPriorityQueue<K extends Comparable<K>, V extends Comparable<V>, P> {
  private final TreeSet<Entry> ordered;
  private Entry last = null;
  private final Map<K, Entry> existence;
  private final int maxSize;
  private int size = 0;

  public CollapsingPriorityQueue(int maxSize) {
    this.maxSize = maxSize;
    ordered = new TreeSet<>();
    existence = new HashMap<>(maxSize);
  }

  /**
   * Fast check to see is the potential key-value pair is competitive by looking only at the value.
   * In cases where it is costly to resolve the key, this method can be used to weed out poorly fitting
   * candidates early.
   * @param value will be compared to the least fitting value for any key in the queue.
   * @return true if the potential key-value pair is a candidate for inclusion in the queue.
   */
  public boolean isCandidate(V value) {
    return size < maxSize || value.compareTo(last.getValue()) > 0;
  }

  /**
   * Add the key-value pair if it is better fitting than the currently poorest fit or update the value if a
   * pair is already present with the same key.
   * @return true if the queue was modified.
   */
  public boolean add(K key, V value) {
    return add(key, value, null);
  }

  /**
   * Add the key-value-payload tuple if it is better fitting than the currently poorest fit or update the value
   * and payload if a tuple is already present with the same key.
   * @return true if the queue was modified.
   */
  public boolean add(K key, V value, P payload) {
    if (!isCandidate(value)) {
      return false;
    }
    Entry existing = existence.get(key);
    if (existing == null) {
      addNew(key, value, payload);
//      System.out.println("New: k=" + key + ", v=" + value + ", p=" + payload + ": " + ordered);
    } else {
      updateExisting(existing, value, payload);
//      System.out.println("Upd: k=" + key + ", v=" + value + ", p=" + payload + ": " + ordered);
    }
    return true;
  }

  // The key is not present
  protected Entry addNew(K key, V value, P payload) {
    Entry newEntry = new Entry(key, value, payload);
    if (size >= maxSize) { // Not enough room: Push out the least fitting
      Entry lastEntry = ordered.last();
      ordered.remove(lastEntry);
      existence.remove(lastEntry.getKey());
    } else {
      size++;
    }
    existence.put(key, newEntry);
    ordered.add(newEntry);
    last = ordered.last();
    return newEntry;
  }

  // Key is present
  protected void updateExisting(Entry existing, V newValue, P newPayload) {
    if (existing.value.compareTo(newValue) > 0) { // TODO: Should we compare payloads here?
      return;
    }
    boolean wasPresent = ordered.remove(existing);
    if (!wasPresent) {
      throw new IllegalStateException("Unable to locate and remove " + existing + " from ordered structure");
    }
    existing.setValue(newValue);
    existing.setPayload(newPayload);
    ordered.add(existing);
    last = ordered.last();
  }

  /**
   * @return the ordered entries.
   */
  public Collection<Entry> getEntries() {
    return ordered;
  }

  public int size() {
    return size;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public class Entry implements Comparable<Entry> {
    private final K key;
    private V value;
    private P payload; // Fully optional

    public Entry(K key, V value) {
      this(key, value, null);
    }

    public Entry(K key, V value, P payload) {
      this.key = key;
      this.value = value;
      this.payload = payload;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    public void setValue(V value) {
      this.value = value;
    }

    public P getPayload() {
      return payload;
    }

    public void setPayload(P payload) {
      this.payload = payload;
    }

    @Override
    public int hashCode() {
      return key.hashCode()+value.hashCode();
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object o) {
      return this == o || (o != null && getClass() == o.getClass() && hashCode() == o.hashCode());
/*      Entry entry = (Entry) o;
      return !((key != null ? !key.equals(entry.key) : entry.key != null) ||
          (value != null ? !value.equals(entry.value) : entry.value != null));*/

    }

    @Override
    public int compareTo(Entry o) {
      int primary = -value.compareTo(o.getValue()); // Highest before lowest
      return primary != 0 ? primary : key.compareTo(key);
    }

    @Override
    public String toString() {
      return "Entry(key=" + key + ", value=" + value + ", payload=" + payload + ')';
    }
  }
}
