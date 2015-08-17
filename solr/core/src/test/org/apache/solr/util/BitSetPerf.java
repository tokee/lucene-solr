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

package org.apache.solr.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UnknownFormatConversionException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.OldFixedBitSet;

/** Performance tester for FixedBitSet.
 * Use -Xbatch for more predictable results, and run tests such that the duration
 * is at least 10 seconds for better accuracy.  Close browsers on your system (javascript
 * or flash may be running and cause more erratic results).
 *
 *
 */
public class BitSetPerf {
  static Random rand = new Random(0);

  public static final String TESTS = "union, cardinality, get, icount, nextSetBit, iterator, clone";

  private int bitSetSize;
  private int numSets;
  private int numBitsSet;
  private List<String> tests;
  private int iter;
  private List<BitSet> javaSets = new ArrayList<>();
  private Map<String, List<org.apache.lucene.util.BitSet>> luceneSets = new HashMap<>();
  private final List<String> implementations;

  public BitSetPerf(
      int bitSetSize, int numSets, int numBitsSet, List<String> tests, int interations, List<String> implementations) {
    this.bitSetSize = bitSetSize;
    this.numSets = numSets;
    this.numBitsSet = numBitsSet;
    this.tests = tests;
    this.iter = interations;
    this.implementations = implementations;
    createSets(implementations);
    fillSets();
  }

  private void createSets(List<String> implementations) {
    for (int i = 0 ; i < numSets ; i++) {
      javaSets.add(new BitSet(bitSetSize));
    }

    for (String implementation: implementations) {
      if ("java".equals(implementation)) {
        continue; // Always created
      }

      List<org.apache.lucene.util.BitSet> sets = new ArrayList<>(numSets);
      for (int i = 0 ; i < numSets ; i++) {
        switch (implementation) {
          case "open": {
            sets.add(new OldFixedBitSet(bitSetSize));
            break;
          }
          case "tracked": {
            sets.add(new FixedBitSet(bitSetSize));
            break;
          }
          default: throw new UnsupportedOperationException("Implementation '" + implementation + "' is unknown");
        }
      }
      this.luceneSets.put(implementation, sets);
    }
  }

  private void fillSets() {
    int idx;
    for (int s = 0 ; s < numSets ; s++) {
      BitSet javaSet = javaSets.get(s);
      for (int si = 0; si < numBitsSet; si++) {
        do {
          idx = rand.nextInt(bitSetSize);
          if (!javaSet.get(idx)) {
            javaSet.set(idx);
            break;
          }
        } while (true);
      }
      for (List<org.apache.lucene.util.BitSet> luceneSets: this.luceneSets.values()) {
        copy(luceneSets.get(s), javaSet);
      }
    }
  }

  private void copy(org.apache.lucene.util.BitSet bitSet, BitSet javaSet) {
    for (int i = 0 ; i < bitSet.length() ; i++) {
      if (bitSet.get(i)) {
        javaSet.set(i);
      }
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length<5) {
      System.out.println("BitSetTest <bitSetSize> <numSets> <numBitsSet> <testName> <iter> <impl>");
      System.out.println("  impl => open=OldFixedBitSet, tracked=FixedBitSet, java=BitSet");
      System.out.println("  tests => " + TESTS);
      return;
    }
    int bitSetSize = Integer.parseInt(args[0]);
    int numSets = Integer.parseInt(args[1]);
    int numBitsSet = Integer.parseInt(args[2]);
    String test = args[3];
    int iter = Integer.parseInt(args[4]);
    String impl = args.length>5 ? args[5].intern() : "java,open,tracked";

    BitSetPerf bitSetPerf = new BitSetPerf(
        bitSetSize, numSets, numBitsSet, Arrays.asList(test.split(" *, *")), iter, Arrays.asList(impl.split(" *, *")));
    bitSetPerf.runTests();
  }

  public void runTests() throws IOException {
    for (String test: tests) {
      System.out.println(String.format("--- sets=%d, iterations=%d, test=%s ---", numSets, iter, test));
      for (String impl: implementations) {
        runTest(test, impl);
      }
    }
  }

  private long runTest(String test, String impl) throws IOException {
    switch (test) {
      case "union": return union(impl);
      case "cardinality": return cardinality(impl);
      case "get": return get(impl);
      case "icount": return icount(impl);
      case "nextSetBit": return nextSetBit(impl);
      case "iterator": return iterator(impl);
      case "clone": return clone(impl);
      default: throw new UnsupportedOperationException("Unknown test: " + test);
    }
  }

  public int union(String impl) throws IOException {
    List<org.apache.lucene.util.BitSet> luceneSet = luceneSets.get(impl);
    final RTimer timer;
    if (luceneSet != null) {
      List<org.apache.lucene.util.BitSet> offsets = offsetClone(luceneSet);
      timer = new RTimer();
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets; i++) {
          offsets.get(i).or(BitSetIterator.getIterator(luceneSet.get(i), luceneSet.get(i).length()));
        }
      }
    } else {
      List<BitSet> offsets = offsetCloneJava(javaSets);
      timer = new RTimer();
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets; i++) {
          offsets.get(i).or(javaSets.get(i));
        }
      }
    }
    System.out.println(String.format("time/iteration=%5fms, check=%d, impl=%s", timer.getTime()/iter, 0, impl));
    return 0;
  }

  public long cardinality(String impl) throws IOException {
    List<org.apache.lucene.util.BitSet> luceneSet = luceneSets.get(impl);
    final RTimer timer = new RTimer();
    long ret = 0;

    for (int it=0; it<iter; it++) {
      for (int i=0; i<numSets; i++) {
        if (luceneSet != null) {
          ret += luceneSet.get(i).cardinality();
        } else {
          ret += javaSets.get(i).cardinality();
        }
      }
    }
    System.out.println(String.format("time/iteration=%5fms, check=%d, impl=%s", timer.getTime()/iter, ret, impl));
    return ret;
  }

  public long get(String impl) throws IOException {
    List<org.apache.lucene.util.BitSet> luceneSet = luceneSets.get(impl);
    final RTimer timer = new RTimer();
    long ret = 0;
    for (int it=0; it<iter; it++) {
      for (int i=0; i<numSets; i++) {
        if (luceneSet != null) {
          org.apache.lucene.util.BitSet oset = luceneSet.get(i);
          for (int k=0; k<bitSetSize; k++) if (oset.get(k)) ret++;
        } else {
          BitSet bset = javaSets.get(i);
          for (int k=0; k<bitSetSize; k++) if (bset.get(k)) ret++;
        }
      }
    }
    System.out.println(String.format("time/iteration=%5fms, check=%d, impl=%s", timer.getTime()/iter, ret, impl));
    return ret;
  }

  public long icount(String impl) throws IOException {
    List<org.apache.lucene.util.BitSet> luceneSet = luceneSets.get(impl);
    final RTimer timer;
    long ret = 0;
    if (luceneSet != null) {
      List<org.apache.lucene.util.BitSet> offsets = offsetClone(luceneSet);
      timer = new RTimer();
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets; i++) {
          org.apache.lucene.util.BitSet set = luceneSet.get(i);
          if (set instanceof FixedBitSet) {
            ret += FixedBitSet.intersectionCount((FixedBitSet)set, (FixedBitSet)offsets.get(i));
          } else if (set instanceof OldFixedBitSet) {
            ret += OldFixedBitSet.intersectionCount((OldFixedBitSet)set, (OldFixedBitSet)offsets.get(i));
          } else {
            throw new UnknownFormatConversionException("Unknown Lucene BitSet class " + set.getClass());
          }
        }
      }
    } else {
      List<BitSet> offsets = offsetCloneJava(javaSets);
      timer = new RTimer();
      for (int it=0; it<iter; it++) {
        for (int i=0; i<numSets; i++) {
          BitSet a=javaSets.get(i);
          BitSet b=offsets.get(i);
          BitSet newset = (BitSet)a.clone();
          newset.and(b);
          ret += newset.cardinality();
        }
      }
    }
    System.out.println(String.format("time/iteration=%5fms, check=%d, impl=%s", timer.getTime()/iter, ret, impl));
    return ret;
  }

  public long nextSetBit(String impl) throws IOException {
    List<org.apache.lucene.util.BitSet> luceneSet = luceneSets.get(impl);
    final RTimer timer = new RTimer();
    long ret = 0;
    for (int it=0; it<iter; it++) {
      for (int i=0; i<numSets; i++) {
        if (luceneSet != null) {
          org.apache.lucene.util.BitSet set = luceneSet.get(i);
          for(int next=set.nextSetBit(0); next != DocIdSetIterator.NO_MORE_DOCS; next=set.nextSetBit(next+1)) {
            ret += next;
          }
        } else {
          BitSet set = javaSets.get(i);
          for(int next=set.nextSetBit(0); next>=0; next=set.nextSetBit(next+1)) {
            ret += next;
          }
        }
      }
    }
    System.out.println(String.format("time/iteration=%5fms, check=%d, impl=%s", timer.getTime()/iter, ret, impl));
    return ret;
  }

  public long iterator(String impl) throws IOException {
    List<org.apache.lucene.util.BitSet> luceneSet = luceneSets.get(impl);
    final RTimer timer = new RTimer();
    long ret = 0;
    for (int it=0; it<iter; it++) {
      for (int i=0; i<numSets; i++) {
        if (luceneSet != null) {
          org.apache.lucene.util.BitSet set = luceneSet.get(i);
          final DocIdSetIterator iterator = BitSetIterator.getIterator(set, 0);
          try {
            for(int next=iterator.nextDoc(); next != BitSetIterator.NO_MORE_DOCS; next=iterator.nextDoc()) {
              ret += next;
            }
          } catch (Exception e) {
            throw new RuntimeException("Unexpected exception iterating FixedBitSet", e);
          }
        } else {
          BitSet set = javaSets.get(i);
          for(int next=set.nextSetBit(0); next>=0; next=set.nextSetBit(next+1)) {
            ret += next;
          }
        }
      }
    }
    System.out.println(String.format("time/iteration=%5fms, check=%d, impl=%s", timer.getTime()/iter, ret, impl));
    return ret;
  }

  public long clone(String impl) throws IOException {
    List<org.apache.lucene.util.BitSet> luceneSet = luceneSets.get(impl);
    final RTimer timer = new RTimer();
    long ret = 0;
    for (int it=0; it<iter; it++) {
      for (int i=0; i<numSets; i++) {
        if (luceneSet != null) {
          luceneSet.set(i, clone(luceneSet.get(i)));
        } else {
          javaSets.set(i, (BitSet) javaSets.get(i).clone());
        }
      }
    }
    System.out.println(String.format("time/iteration=%5fms, check=%d, impl=%s", timer.getTime()/iter, ret, impl));
    return ret;
  }

  private List<org.apache.lucene.util.BitSet> offsetClone(List<org.apache.lucene.util.BitSet> luceneSets) {
    List<org.apache.lucene.util.BitSet> offsets = new ArrayList<>(luceneSets.size());
    for (int i = 1 ; i < luceneSets.size() ; i++) {
      offsets.add(clone(luceneSets.get(i)));
    }
    offsets.add(clone(luceneSets.get(0)));
    return offsets;
  }

  private org.apache.lucene.util.BitSet clone(org.apache.lucene.util.BitSet set) {
    if (set instanceof FixedBitSet) {
      return ((FixedBitSet)set).clone();
    } else if (set instanceof OldFixedBitSet) {
        return ((OldFixedBitSet)set).clone();
    } else {
      throw new UnknownFormatConversionException("Unknown Lucene BitSet class " + set.getClass());
    }
  }

  private List<BitSet> offsetCloneJava(List<BitSet> javaSets) {
    List<BitSet> offsets = new ArrayList<>(javaSets.size());
    for (int i = 1 ; i < javaSets.size() ; i++) {
      offsets.add((BitSet)javaSets.get(i).clone());
    }
    offsets.add((BitSet)javaSets.get(0).clone());
    return offsets;
  }
}
