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

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.sparse.cache.SparseCounterPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for ordinal processing. Used by sparse faceting.
 */
public class OrdinalUtils {
  private static Logger log = LoggerFactory.getLogger(OrdinalUtils.class);

  /**
   * Memory and CPU-power heavy extraction of counts for all global ordinals for the given field.
   * This should be called as few times as possible. Consider using {@link #getBPVs} instead.
   */
  public static int[] getGlobalOrdinalCount(
      SolrIndexSearcher searcher, SortedSetDocValues si, MultiDocValues.OrdinalMap globalMap,
      SchemaField schemaField) throws IOException {
    final long startTime = System.nanoTime();
    int valueCount = (int) si.getValueCount();
    if (valueCount > 10*1000*1000) { // Counting time will probably not be trivial
      log.info(
          "Extracting global ordinal count for field " + schemaField.getName() + " with " + valueCount
              + " values. Temporary memory overhead will be " + 1L*valueCount*Integer.SIZE/8/1024/1024 + "MB");
    }
    final int[] globOrdCount = new int[valueCount+1]; // TODO: Why do we need the +1?
    List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
    for (int subIndex = 0; subIndex < leaves.size(); subIndex++) {
      final LongValues globalOrds = globalMap.getGlobalOrds(subIndex);

      final LeafReaderContext leaf = leaves.get(subIndex);
      final Bits live = leaf.reader().getLiveDocs();
      if (schemaField.multiValued()) {
        // Field marked as multi-valued, but in reality it might be single-valued
        SortedSetDocValues sub = leaf.reader().getSortedSetDocValues(schemaField.getName());
        if (sub == null) {
          sub = DocValues.emptySortedSet();
        }
        final SortedDocValues singleton = DocValues.unwrapSingleton(sub);
        if (singleton != null) {
          // A maximum of 1 term/document
          singleton.nextDoc();
          int docID;
          while ((docID = singleton.docID()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (live == null || live.get(docID)) {
              int segmentOrd = singleton.ordValue();
              if (segmentOrd >= 0) {
                long globalOrd = globalMap == null ? segmentOrd :
                    // getGlobalOrds(subIndex) should be very fast (double indirect array look up)
                    // Still, it might be worth it to cache it at the beginning of the subIndex-loop
                    (int) globalMap.getGlobalOrds(subIndex).get(segmentOrd);

//                long globalOrd = globalMap == null ? segmentOrd : globalMap.getGlobalOrd(subIndex, segmentOrd);
                if (globalOrd >= 0) {
                  // Not liking the cast here, but 2^31 is de facto limit for most structures
                  globOrdCount[((int) globalOrd)]++;
                }
              }
            }
          }
        } else {
          // Multiple terms/document
          sub.nextDoc();
          int docID;
          while ((docID = sub.docID()) != DocIdSetIterator.NO_MORE_DOCS) {
            if (live == null || live.get(docID)) {
              // strange do-while to collect the missing count (first ord is NO_MORE_ORDS)
              int ord = (int) sub.nextOrd();
              if (ord < 0) {
                continue;
              }

              do {
                if (globalMap != null) {
                  ord = (int) globalMap.getGlobalOrds(subIndex).get(ord);
                }
                if (ord >= 0) {
                  globOrdCount[ord]++;
                }
              } while ((ord = (int) sub.nextOrd()) >= 0);
            }
          }
        }
      } else {
        // Marked as single-valued
        SortedDocValues sub = leaf.reader().getSortedDocValues(schemaField.getName());

        if (sub == null) {
          sub = DocValues.emptySorted();
        }
        sub.nextDoc();
        int docID;
        while ((docID = sub.docID()) != DocIdSetIterator.NO_MORE_DOCS) {
          if (live == null || live.get(docID)) {
            int segmentOrd = sub.ordValue();
            if (segmentOrd >= 0) {
//              long globalOrd = globalMap == null ? segmentOrd : globalMap.getGlobalOrd(subIndex, segmentOrd);
              long globalOrd = globalMap == null ? segmentOrd :
                  (int) globalMap.getGlobalOrds(subIndex).get(segmentOrd);
              if (globalOrd >= 0) {
                // Not liking the cast here, but 2^31 is de facto limit for most structures so we win nothing with long
                globOrdCount[((int) globalOrd)]++;
              }
            }
          }
        }
      }
    }
    log.info(String.format(Locale.ENGLISH,
        "Extracted global ord count for field %s with %d unique values in %dms",
        schemaField.getName(), globOrdCount.length, (System.nanoTime() - startTime) / 1000000));
    return globOrdCount;
  }

  // saveMemory implies dual-pass of DocValues ordinals
  public static BPVProvider getBPVs(
      SolrIndexSearcher searcher, SortedSetDocValues si, MultiDocValues.OrdinalMap globalMap, SchemaField schemaField,
      boolean saveMemory) throws IOException {
    if (saveMemory && schemaField.indexed()) { // Indexed
      List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
      if (leaves.size() == 1) { // Single segment
        LeafReaderContext leaf = leaves.get(0);
        // TODO: Add support for deleted documents to save space
        // Bits live = leaf.reader().getLiveDocs();
        Terms terms = leaf.reader().terms(schemaField.getName());
        if (terms != null) {
          log.info("Single segment index detected. Creating memory optimized BitsPerValue " +
              "provider for field " + schemaField.getName() + " with " + terms.size() + " terms");
          return new SingleSegmentOrdinalCounts(terms);
        }
      }
    }
    return new BPVProvider.BPVIntArrayWrapper(
        getGlobalOrdinalCount(searcher, si, globalMap, schemaField), false);
  }

  /**
   * Resolves an ordinal to an external String. The lookup might be cached.
   *
   * @param pool       the Searcher-instance-specific pool, potentially holding a cache of lookup terms.
   * @param sparseKeys the sparse request, stating whether caching of terms should be used or not.
   * @param si         term provider.
   * @param ft         field type for mapping internal term representation to external.
   * @param ordinal    the ordinal for the requested term.
   * @param charsRef   independent CharsRef to avoid object allocation.
   * @return an external String directly usable for constructing facet response.
   */
  // TODO: Remove brNotUsedAnymore
  public static String resolveTerm(SparseCounterPool pool, SparseKeys sparseKeys, SortedSetDocValues si,
                                   FieldType ft, int ordinal, CharsRef charsRef) throws IOException {
    // TODO: Check that the cache spans segments instead of just handling one
    if (sparseKeys.termLookupMaxCache > 0 && sparseKeys.termLookupMaxCache >= si.getValueCount()) {
      BytesRefArray brf = pool.getExternalTerms();
      if (brf == null) {
        // Fill term cache
        brf = new BytesRefArray(Counter.newCounter()); // TODO: Incorporate this in the overall counters
        for (int ord = 0; ord < si.getValueCount(); ord++) {
          BytesRef br = si.lookupOrd(ord);
          ft.indexedToReadable(br, charsRef);
          brf.append(new BytesRef(charsRef)); // Is the object allocation avoidable?
        }
        pool.setExternalTerms(brf);
      }
      // TODO: Re-use the BytesRefBuilder
      return brf.get(new BytesRefBuilder(), ordinal).utf8ToString();
    }
    pool.setExternalTerms(null); // Make sure the memory is freed
    // No cache, so lookup directly
    BytesRef br = si.lookupOrd(ordinal);
    ft.indexedToReadable(br, charsRef);
    return charsRef.toString();
  }

  public static String join(long[] values) {
    StringBuilder sb = new StringBuilder(values.length*5);
    for (long value: values) {
      if (sb.length() != 0) {
        sb.append(", ");
      }
      sb.append(Long.toString(value));
    }
    return sb.toString();
  }

  // Quite the hack with the extra value at the end. There is a 1-off error somewhere yet undiscovered that needs it
  public static class SingleSegmentOrdinalCounts implements BPVProvider {
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
        termsEnum = terms.iterator();
        currentBytesRef = null;
      } catch (IOException e) {
        throw new RuntimeException("Unable to create new TermsEnum iterator from terms", e);
      }
    }
  }

}
