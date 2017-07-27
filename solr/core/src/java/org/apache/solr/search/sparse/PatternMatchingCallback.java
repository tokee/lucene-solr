package org.apache.solr.search.sparse;

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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.CharsRef;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.sparse.cache.SparseCounterPool;
import org.apache.solr.search.sparse.track.ValueCounter;
import org.apache.solr.util.LongPriorityQueue;

/**
 * Slow callback with white- or blacklist of terms. This needs to resolve the String for each otherwise viable
 * candidate.
 */
public class PatternMatchingCallback implements ValueCounter.Callback {
  private int min;
  private final int[] maxTermCounts;
  private final boolean doNegative;
  private final LongPriorityQueue queue;
  private final int queueMaxSize;
  private final SparseKeys sparseKeys;
  private boolean isOrdered = false;
  private final SparseCounterPool pool;
  private final SortedSetDocValues si;
  private final FieldType ft;
  private final CharsRef charsRef;

  private final Matcher[] whiteMatchers;
  private final Matcher[] blackMatchers;

  /**
   * Creates a basic callback where only the values >= min are considered.
   * @param min      the starting min value.
   * @param queue   the destination of the values of the counters.
   */
  public PatternMatchingCallback(
      int min, LongPriorityQueue queue, int queueMaxSize, SparseKeys sparseKeys, SparseCounterPool pool,
      SortedSetDocValues si, FieldType ft, CharsRef charsRef) {
    this.maxTermCounts = null;
    this.min = min;
    this.doNegative = false;
    this.queue = queue;
    this.queueMaxSize = queueMaxSize;
    this.sparseKeys = sparseKeys;
    this.pool = pool;
    this.si = si;
    this.ft = ft;
    this.charsRef = charsRef;
    // Instead of generating new matchers all the time, we create them once and re-use them
    whiteMatchers = generateMatchers(sparseKeys.whitelists);
    blackMatchers = generateMatchers(sparseKeys.blacklists);
  }

  private Matcher[] generateMatchers(List<Pattern> patterns) {
    Matcher[] matchers = new Matcher[patterns.size()];
    for (int i = 0 ; i < patterns.size() ; i++) {
      matchers[i] = patterns.get(i).matcher("dummy");
    }
    return matchers;
  }

  @Override
  public void setOrdered(boolean isOrdered) {
    this.isOrdered = isOrdered;
  }

  @Override
  public final boolean handle(final int counter, final long value) {
    final int c = (int) (doNegative ? maxTermCounts[counter] - value : value);
    if (isOrdered ? c > min : c>=min) {
      // NOTE: Using > only works when the results are delivered in order.
      // The ordered uses c>min rather than c>=min as an optimization because we are going in
      // index order, so we already know that the keys are ordered.  This can be very
      // important if a lot of the counts are repeated (like zero counts would be).

      // smaller term numbers sort higher, so subtract the term number instead
      final long pair = (((long)c)<<32) + (Integer.MAX_VALUE - counter);
      //boolean displaced = queue.insert(pair);
      int regexps = 0;
      if (queue.size() < queueMaxSize || pair > queue.top()) { // Add to queue
        long patternStart = System.nanoTime();
        try {
          //final String term = resolveTerm(pool, sparseKeys, si, ft, counter-1, charsRef, br);
          final String term = OrdinalUtils.resolveTerm(pool, sparseKeys, si, ft, counter, charsRef);
          for (Matcher whiteMatcher: whiteMatchers) {
            regexps++;
            whiteMatcher.reset(term);
            if (!whiteMatcher.matches()) {
              return false;
            }
          }
          for (Matcher blackMatcher: blackMatchers) {
            regexps++;
            blackMatcher.reset(term);
            if (blackMatcher.matches()) {
              return false;
            }
          }
        } finally {
          if (regexps > 0) {
            pool.regexpMatches.incRel(regexps, patternStart);
          }
        }
        if (queue.insert(pair)) {
          min=(int)(queue.top() >>> 32);
          return true;
        }
      }
    }
    return false;
  }
}
