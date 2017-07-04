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

package org.apache.solr.search.sparse.counters.plane;

import java.util.Locale;
import java.util.Random;

import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.search.sparse.counters.plane.RankCache;

public class TestRankCache extends LuceneTestCase {

  public void testSingleWord() {
    RankCache rank = createCache(60L, 20);

    assertEquals("The rank at index 20 should be correct", 0, rank.rank(20));
    assertEquals("The rank at index 21 should be correct", 1, rank.rank(21));
  }

  private RankCache createCache(long bitCount, int... setBits) {
    FixedBitSet bitSet = new FixedBitSet((int) bitCount);
    for (int setBit: setBits) {
      bitSet.set(setBit);
    }
    return new RankCache(bitSet);
  }

  public void testSecondWord() {
    RankCache rank = createCache(100L, 70);

    assertEquals("The rank at index 70 should be correct", 0, rank.rank(70));
    assertEquals("The rank at index 71 should be correct", 1, rank.rank(71));
  }

  public void testThirdWord() {
    RankCache rank = createCache(200L, 130);

    assertEquals("The rank at index 130 should be correct", 0, rank.rank(130));
    assertEquals("The rank at index 131 should be correct", 1, rank.rank(131));
  }

  public void testSecondLower() {
    RankCache rank = createCache(3000L, 2500);

    assertEquals("The rank at index 2500 should be correct", 0, rank.rank(2500));
    assertEquals("The rank at index 2501 should be correct", 1, rank.rank(2501));
    assertEquals("The rank at index 2502 should be correct", 1, rank.rank(2502));
  }

  public void testSpecific282() {
    RankCache rank = createCache(448L, 282);

    assertEquals("The rank at index 288 should be correct", 1, rank.rank(288));
  }

  public void testSpecific1031() {
    RankCache rank = createCache(1446L, 1031);

    assertEquals("The rank at index 1057 should be correct", 1, rank.rank(1057));
  }

  public void testSmallMonkey() {
    monkey(5, 3000, 10);
  }

  public void testMoreMonkeys() {
    monkey(20, 8000, 40);
  }

  @Slow
  public void testManyMonkeys() {
    monkey(20, 100000, 400);
  }

  public void monkey(int runs, int sizeMax, int setMax) {
    Random random = random();
    //Random random = new Random(87);
    for (int run = 0 ; run < runs ; run++) {
      final int size = random.nextInt(sizeMax-1)+1;
      FixedBitSet bitSet = new FixedBitSet(size);
      int doSet = random.nextInt(setMax);
      for (int s = 0 ; s < doSet ; s++) {
        int index = random.nextInt(size);
        bitSet.set(index);
      }
      RankCache rank = new RankCache(bitSet);
      int setbits = 0;
      for (int i = 0 ; i < size ; i++) {
        assertEquals(String.format(Locale.ENGLISH, "run=%d, index=%d/%d, setbits=%d", run, i, size, setbits),
            setbits, rank.rank(i));
        if (bitSet.get(i)) {
          setbits++;
        }
      }
    }
  }
}
