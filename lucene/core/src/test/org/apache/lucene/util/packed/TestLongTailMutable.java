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

@Slow
public class TestLongTailMutable extends LuceneTestCase {

  private final static int M = 1048576;

  public void testLinksEstimate() {
    testEstimate(400*M, getLinksHistogram());
  }

  public void testURLShard1Estimate() {
    testEstimate(228*M, getURLSampleHistogram());
  }

  public void testViability() {
    LongTailMutable.Estimate estimate = new LongTailMutable.Estimate(100*M, pad(
        1000,
        100,
        1,
        3
    ));
    // 1+3=4 and there are 2^2=4 pointers
    assertTrue("The layout should be viable for tailBPV=2", estimate.isViable(2));
  }

  public void testNonViability() {
    LongTailMutable.Estimate estimate = new LongTailMutable.Estimate(100*M, pad(
        1000,
        100,
        2,
        3
    ));
    // 2+3=5 and there are 2^2=4 pointers
    assertFalse("The layout should not be viable for tailBPV=2", estimate.isViable(2));
  }

  public void testEstimate(long uniqueCount, long[] histogram) {
    LongTailMutable.Estimate estimate = new LongTailMutable.Estimate(uniqueCount, histogram);
    for (int tailBPV = 0 ; tailBPV < 64 ; tailBPV++) {
      if (estimate.isViable(tailBPV) && estimate.getFractionEstimate(tailBPV) < 1.1) {
        System.out.println(String.format(Locale.ENGLISH,
            "tailBPV=%2d men=%4.2fGB/%4.2fGB fraction=%4.2f headValueCount=%4d (%6.4f%%)",
            tailBPV, estimate.getMemEstimate(tailBPV) / 1024.0 / 1024 / 1024,
            0.400*estimate.getMaxBPV()/8,
            estimate.getFractionEstimate(tailBPV), estimate.getHeadValueCount(tailBPV),
            estimate.getHeadValueCount(tailBPV)*100.0/uniqueCount));
      }
    }
  }

  private long[] getURLSampleHistogram() { // Taken from URL from shard 1 in netarchive.dk
    // 228M uniques
    return pad(
        196552211,
        20504581,
        5626432,
        3187738,
        1123002,
        411353,
        164206,
        81438,
        10421, // 256
        2252,
        2526,
        100,
        6      // 4096
    );
  }
  private long[] getLinksHistogram() {
    return pad( // Taken from links in a 2/3 build shard from netarchive.dk
        // 400M uniques
        324916865,
        57336093,
        37200810,
        22636130,
        12677476,
        7074694,
        3830153,
        2317588,
        1439165, // 256
        875950,
        541195,
        324288,
        180056,
        74934,
        26037,
        5630,
        2363,    // 65536
        643,
        619,
        335,
        99,
        5
    );
  }

  private long[] pad(long... maxCounts) {
    long[] full = new long[64];
    for (int i = 0 ; i < maxCounts.length ; i++) {
      full[i] = maxCounts[i];
    }
    return full;
  }
}
