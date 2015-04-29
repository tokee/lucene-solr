package org.apache.solr.request.sparse;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.NPlaneMutable;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Helper methods for ordinal processing. Used by sparse faceting.
 */
public class OrdinalUtils {

  /*
    Memory and CPU-power heavy extraction of counts for all global ordinals.
    This should be called as few times as possible.
     */
  public static int[] getGlobalOrdinalCount(
      SolrIndexSearcher searcher, SortedSetDocValues si, MultiDocValues.OrdinalMap globalMap,
      SchemaField schemaField) throws IOException {
    final long startTime = System.nanoTime();
    int valueCount = (int) si.getValueCount();
    if (valueCount > 10*1000*1000) { // Counting time will probably not be trivial
      SparseDocValuesFacets.log.info(
          "Extracting global ordinal count for field " + schemaField.getName() + " with " + valueCount
              + " values. Temporary memory overhead will be " + 1L*valueCount*Integer.SIZE/8/1024/1024 + "MB");
    }
    final int[] globOrdCount = new int[valueCount+1]; // TODO: Why do we need the +1?
    List<AtomicReaderContext> leaves = searcher.getTopReaderContext().leaves();
    for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
      AtomicReaderContext leaf = leaves.get(subIndex);
      Bits live = leaf.reader().getLiveDocs();
      if (schemaField.multiValued()) {
        SortedSetDocValues sub = leaf.reader().getSortedSetDocValues(schemaField.getName());
        if (sub == null) {
          sub = DocValues.EMPTY_SORTED_SET;
        }
        final SortedDocValues singleton = DocValues.unwrapSingleton(sub);
        if (singleton != null) {
          for (int docID = 0 ; docID < leaf.reader().maxDoc() ; docID++) {
            if (live == null || live.get(docID)) {
              int segmentOrd = singleton.getOrd(docID);
              if (segmentOrd >= 0) {
                long globalOrd = globalMap == null ? segmentOrd : globalMap.getGlobalOrd(subIndex, segmentOrd);
                if (globalOrd >= 0) {
                  // Not liking the cast here, but 2^31 is de facto limit for most structures
                  globOrdCount[((int) globalOrd)]++;
                }
              }
            }
          }
        } else {
          for (int docID = 0 ; docID < leaf.reader().maxDoc() ; docID++) {
            if (live == null || live.get(docID)) {
              sub.setDocument(docID);
              // strange do-while to collect the missing count (first ord is NO_MORE_ORDS)
              int ord = (int) sub.nextOrd();
              if (ord < 0) {
                continue;
              }

              do {
                if (globalMap != null) {
                  ord = (int) globalMap.getGlobalOrd(subIndex, ord);
                }
                if (ord >= 0) {
                  globOrdCount[ord]++;
                }
              } while ((ord = (int) sub.nextOrd()) >= 0);
            }
          }
        }
      } else {
        SortedDocValues sub = leaf.reader().getSortedDocValues(schemaField.getName());
        if (sub == null) {
          sub = DocValues.EMPTY_SORTED;
        }
        for (int docID = 0 ; docID < leaf.reader().maxDoc() ; docID++) {
          if (live == null || live.get(docID)) {
            int segmentOrd = sub.getOrd(docID);
            if (segmentOrd >= 0) {
              long globalOrd = globalMap == null ? segmentOrd : globalMap.getGlobalOrd(subIndex, segmentOrd);
              if (globalOrd >= 0) {
                // Not liking the cast here, but 2^31 is de facto limit for most structures
                globOrdCount[((int) globalOrd)]++;
              }
            }
          }
        }
      }
    }
    SparseDocValuesFacets.log.info(String.format(Locale.ENGLISH,
        "Extracted global ord count for field %s with %d unique values in %dms",
        schemaField.getName(), globOrdCount.length, (System.nanoTime() - startTime) / 1000000));
    return globOrdCount;
  }

  // saveMemory implies dual-pass of DocValues ordinals
  public static NPlaneMutable.BPVProvider getBPVs(
      SolrIndexSearcher searcher, SortedSetDocValues si, MultiDocValues.OrdinalMap globalMap, SchemaField schemaField,
      boolean saveMemory) throws IOException {
    if (saveMemory && schemaField.indexed()) { // Indexed
      List<AtomicReaderContext> leaves = searcher.getTopReaderContext().leaves();
      if (leaves.size() == 1) { // Single segment
        AtomicReaderContext leaf = leaves.get(0);
        // TODO: Add support for deleted documents to save space
        // Bits live = leaf.reader().getLiveDocs();
        Terms terms = leaf.reader().fields().terms(schemaField.getName());
        if (terms != null) {
          SparseDocValuesFacets.log.info("Single segment index detected. Creating memory optimized BitsPerValue " +
              "provider for field " + schemaField.getName() + " with " + terms.size() + " terms");
          return new SingleSegmentOrdinalCounts(terms);
        }
      }
    }
    return new NPlaneMutable.BPVIntArrayWrapper(
        getGlobalOrdinalCount(searcher, si, globalMap, schemaField), false);
  }

  // Quite the  hack with the extra value at the end. There is a 1-off error somewhere yet undiscovered that needs it
  public static class SingleSegmentOrdinalCounts implements NPlaneMutable.BPVProvider {
    private final static BytesRef THE_END = new BytesRef("EOD");
    private final static BytesRef THE_END_POLLED = new BytesRef("EOD-EOL");

    private final Terms terms;
    private TermsEnum termsEnum;

    private BytesRef currentBytesRef;

    public SingleSegmentOrdinalCounts(Terms terms) {
      this.terms = terms;
      reset();
    }

    @Override
    public boolean hasNext() {
      try {
        if (currentBytesRef == THE_END_POLLED) {
          return false;
        }
        if (currentBytesRef == null && (currentBytesRef = termsEnum.next()) == null) {
          currentBytesRef = THE_END;
        }
        return true;
      } catch (IOException e) {
        throw new RuntimeException("Exception calling hasNext on TermsEnum", e);
      }
    }

    @Override
    public int nextBPV() {
      try {
        if (currentBytesRef ==  null && !hasNext()) {
          throw new IllegalStateException("next called without more available values");
        }
        if (currentBytesRef == THE_END) {
          currentBytesRef = THE_END_POLLED;
          return 1;
        }
        // Will a document containing the same term multiple times report 1 or n here?
        currentBytesRef = null;
        return termsEnum.docFreq() == 0 ? 1 : PackedInts.bitsRequired(termsEnum.docFreq());
      } catch (IOException e) {
        throw new RuntimeException("Exception calling next on TermsEnum", e);
      }
    }

    @Override
    public long nextValue() {
      try {
        if (currentBytesRef ==  null && !hasNext()) {
          throw new IllegalStateException("next called without more available values");
        }
        if (currentBytesRef == THE_END) {
          currentBytesRef = THE_END_POLLED;
          return 1;
        }
        // Will a document containing the same term multiple times report 1 or n here?
        currentBytesRef = null;
        return termsEnum.docFreq() == 0 ? 1 : termsEnum.docFreq();
      } catch (IOException e) {
        throw new RuntimeException("Exception calling next on TermsEnum", e);
      }
    }

    @Override
    public void reset() {
      try {
        termsEnum = terms.iterator(termsEnum);
        currentBytesRef = null;
      } catch (IOException e) {
        throw new RuntimeException("Unable to create new TermsEnum iterator from terms", e);
      }
    }
  }

  public static class StatCollectingBPVWrapper implements NPlaneMutable.BPVProvider {
    private final NPlaneMutable.BPVProvider source;

    public long entries = 0;
    public long maxCount = -1;
    public long refCount = 0;
    public final long[] histogram = new long[64];

    public StatCollectingBPVWrapper(NPlaneMutable.BPVProvider source) {
      this.source = source;
    }

    @Override
    public boolean hasNext() {
      return source.hasNext();
    }

    @Override
    public int nextBPV() {
      final long ordCount = (int) source.nextValue();
      final int bpv = PackedInts.bitsRequired(ordCount);
      { // Statistics collection
        histogram[bpv]++;
        refCount += ordCount;
        if (ordCount > maxCount) {
          maxCount = ordCount;
        }
        entries++;
      }
      return bpv;
    }
    @Override
    public long nextValue() {
      final long ordCount = (int) source.nextValue();
      { // Statistics collection
        final int bpv = PackedInts.bitsRequired(ordCount);
        histogram[bpv]++;
        refCount += ordCount;
        if (ordCount > maxCount) {
          maxCount = ordCount;
        }
        entries++;
      }
      return ordCount;
    }

    @Override
    public void reset() {
      source.reset();
      entries = 0;
      maxCount = -1;
      refCount = 0;
      Arrays.fill(histogram, 0);
    }

    /**
     * Convenience method if the only purpose is to collect statistics.
     */
    public void collect() {
      while (hasNext()) {
        nextValue();
      }
    }

    @Override
    public String toString() {
      return "StatCollectingBPVWrapper(" +
          "entries=" + entries + ", maxCount=" + maxCount + ", refCount=" + refCount +
          ", histogram=" + Arrays.toString(histogram) + ')';
    }
  }
}
