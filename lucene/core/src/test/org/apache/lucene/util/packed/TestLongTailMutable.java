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
    testEstimate("8/9 shard links", 519*M, getLinksHistogram(), false);
  }

  public void testURLEstimate() {
    testEstimate("Shard 1 URL", 228*M, getURLShard1Histogram(), true);
    testEstimate("Shard 2 URL", 230*M, getURLShard2Histogram(), false);
    testEstimate("Shard 3 URL", 214*M, getURLShard3Histogram(), false);
  }
  public void testHostEstimate() {
    testEstimate("Shard 1 host", 1562680, SHARD1_HOST, false);
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

  public void testEstimate(String designation, long uniqueCount, long[] histogram, boolean table) {
    LongTailMutable.Estimate estimate = new LongTailMutable.Estimate(uniqueCount, histogram);
    final double PACKED_MB = uniqueCount*estimate.getMaxBPV()/8 / 1024.0 / 1024;
    System.out.println(String.format(Locale.ENGLISH,
        table ?
            "<table style=\"width: 80%%\"><caption>%s: %s uniques, Packed64 size: %.0fMB</caption>\n" +
                "<tr style=\"text-align: right\"><th>tailBPV</th> <th>mem</th> <th>saved</th> <th>headCounters</th></tr>" :
            "%s: %s uniques, Packed64 size: %.0fMB",
        designation, uniqueCount < 10*M ? uniqueCount/1000 + "K>" : uniqueCount/M + "M", PACKED_MB));
    for (int tailBPV = 0 ; tailBPV < 64 ; tailBPV++) {
      if (estimate.isViable(tailBPV) && estimate.getFractionEstimate(tailBPV) <= 1.0) {
        double mb = estimate.getMemEstimate(tailBPV) / 1024.0 / 1024;
        System.out.println(String.format(Locale.ENGLISH,
            table ?
                "<tr style=\"text-align: right\"><td>%2d</td> <td>%4.0fMB</td> <td>%3.0fMB / %2.0f%%</td> <td>%4d</td></tr>" :
                "tailBPV=%2d mem=%4.0fMB (%3.0fMB / %2.0f%% saved) headCounters=%4d (%6.4f%%)",
            tailBPV, mb, PACKED_MB-mb,
            (1-estimate.getFractionEstimate(tailBPV))*100, estimate.getHeadValueCount(tailBPV),
            estimate.getHeadValueCount(tailBPV)*100.0/uniqueCount));
      }
    }
    if (table) {
      System.out.println("</table>");
    }
  }

  private long[] getURLShard1Histogram() { // Taken from URL from shard 1 in netarchive.dk
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

  private long[] SHARD1_HOST = pad(
      // 1562680 uniques
      194715,
      181562,
      178020,
      177279,
      168170,
      129294,
      90654,
      60348,
      39070,
      22778,
      13934,
      8515,
      5829,
      3876,
      1799,
      556,
      153,
      44,
      22,
      17,
      6,
      3,
      1
  );

  private long[] getURLShard2Histogram() { // Taken from URL in netarchive.dk
    // 230M uniques
    return pad(
        213596462,
        7814717,
        3496320,
        2446379,
        1370559,
        474296,
        207471,
        91941,
        18593,
        3467,
        2754,
        150,
        7
    );
  }
  private long[] getURLShard3Histogram() { // Taken from URL in netarchive.dk
    // 214M uniques
    return pad(
        188429984,
        17107714,
        4977514,
        2431354,
        946224,
        361199,
        140717,
        65064,
        6183,
        2278,
        1996,
        64,
        3
    );
  }


  private long[] getLinksHistogram() {
    return pad( // Taken from links in a 8/9 build shard from netarchive.dk
        // 519M uniques
        351962313,
        64785381,
        42315979,
        26072745,
        14361240,
        8035509,
        4611177,
        2686949,
        1659800,
        1005294,
        632518,
        368445,
        208885,
        94266,
        32975,
        8196,
        2807,
        1755,
        465,
        540,
        311,
        58
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
