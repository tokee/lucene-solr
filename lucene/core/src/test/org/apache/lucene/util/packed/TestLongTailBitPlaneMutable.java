package org.apache.lucene.util.packed;

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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;

import java.util.Locale;
import java.util.Random;

@Slow
public class TestLongTailBitPlaneMutable extends LuceneTestCase {

  private final static int M = 1048576;

  public void testSmall() {
    final int[] MAXIMA = new int[]{10, 1, 16, 2, 3};
    final int MAX = 16;
    final PackedInts.Mutable maxima =
        PackedInts.getMutable(MAXIMA.length, PackedInts.bitsRequired(MAX), PackedInts.COMPACT);
    for (int i = 0 ; i < MAXIMA.length ; i++) {
      maxima.set(i, MAXIMA[i]);
    }

    PackedInts.Mutable bpm = new LongTailBitPlaneMutable(maxima);
    bpm.set(1, bpm.get(1)+1);
    assertEquals("Test 1: index 1", 1, bpm.get(1));
    bpm.set(0, bpm.get(0)+1);
    assertEquals("Test 2: index 0", 1, bpm.get(0));
    bpm.set(0, bpm.get(0)+1);
    bpm.set(0, bpm.get(0)+1);
    assertEquals("Test 3: index 0", 3, bpm.get(0));
  }

  public void testRandom() {
    final int COUNTERS = 100;
    final int MAX = 1000;
    final int updates = M;
    final PackedInts.Reader maxima = getMaxima(COUNTERS, MAX);

    PackedInts.Mutable expected = PackedInts.getMutable(COUNTERS, PackedInts.bitsRequired(MAX), PackedInts.FASTEST);
    PackedInts.Mutable bpm = new LongTailBitPlaneMutable(maxima);
    for (int update = 0 ; update < updates ; update++) {
      int index = random().nextInt(COUNTERS);
      System.out.println("Update " + update + " with index " + index);
      while (expected.get(index) >= maxima.get(index)) {
        index++;
        if (index == COUNTERS) {
          index = 0;
        }
      }
      expected.set(index, expected.get(index));
      bpm.set(index, bpm.get(index));
      assertEquals("After " + (update+1) + " updates the BPM-value should be as expected",
          expected.get(index), bpm.get(index));
    }
  }

  private PackedInts.Reader getMaxima(int counters, int max) {
    final PackedInts.Mutable maxima = PackedInts.getMutable(counters, 30, PackedInts.FASTEST);
    for (int i = 0 ; i < counters ; i++) {
      maxima.set(i, random().nextInt(max-1)+1);
    }
    return maxima;
  }

}
