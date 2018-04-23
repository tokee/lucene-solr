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
package org.apache.solr.search.facet;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.index.SlowCompositeReaderWrapper;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TrieField;
import org.apache.solr.search.BitDocSet;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.sparse.SparseKeys;
import org.apache.solr.search.sparse.cache.SparseCounterPool;
import org.apache.solr.search.sparse.track.ValueCounter;
import org.apache.solr.uninverting.DocTermOrds;
import org.apache.solr.util.LongPriorityQueue;
import org.apache.solr.util.PrimUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Final form of the un-inverted field:
 *   Each document points to a list of term numbers that are contained in that document.
 *
 *   Term numbers are in sorted order, and are encoded as variable-length deltas from the
 *   previous term number.  Real term numbers start at 2 since 0 and 1 are reserved.  A
 *   term number of 0 signals the end of the termNumber list.
 *
 *   There is a single int[maxDoc()] which either contains a pointer into a byte[] for
 *   the termNumber lists, or directly contains the termNumber list if it fits in the 4
 *   bytes of an integer.  If the first byte in the integer is 1, the next 3 bytes
 *   are a pointer into a byte[] where the termNumber list starts.
 *
 *   There are actually 256 byte arrays, to compensate for the fact that the pointers
 *   into the byte arrays are only 3 bytes long.  The correct byte array for a document
 *   is a function of its id.
 *
 *   To save space and speed up faceting, any term that matches enough documents will
 *   not be un-inverted... it will be skipped while building the un-inverted field structure,
 *   and will use a set intersection method during faceting.
 *
 *   To further save memory, the terms (the actual string values) are not all stored in
 *   memory, but a TermIndex is used to convert term numbers to term values only
 *   for the terms needed after faceting has completed.  Only every 128th term value
 *   is stored, along with its corresponding term number, and this is used as an
 *   index to find the closest term and iterate until the desired number is hit (very
 *   much like Lucene's own internal term index).
 *
 */
public class UnInvertedField extends DocTermOrds {
  private static int TNUM_OFFSET=2;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static class TopTerm {
    Query termQuery;
    BytesRef term;
    int termNum;

    long memSize() {
      return 8 +   // obj header
          8 + 8 +term.length +  //term
          4;    // int
    }
  }

  long memsz;
  final AtomicLong use = new AtomicLong(); // number of uses

  int[] maxTermCounts = new int[1024];

  final Map<Integer,TopTerm> bigTerms = new LinkedHashMap<>();

  private SolrIndexSearcher.DocsEnumState deState;
  private final SolrIndexSearcher searcher;

  private static UnInvertedField uifPlaceholder = new UnInvertedField();

  private UnInvertedField() { // Dummy for synchronization.
    super("fake", 0, 0); // cheapest initialization I can find.
    searcher = null;
  }

  @Override
  protected void visitTerm(TermsEnum te, int termNum) throws IOException {

    if (termNum >= maxTermCounts.length) {
      // resize by doubling - for very large number of unique terms, expanding
      // by 4K and resultant GC will dominate uninvert times.  Resize at end if material
      int[] newMaxTermCounts = new int[ Math.min(Integer.MAX_VALUE-16, maxTermCounts.length*2) ];
      System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, termNum);
      maxTermCounts = newMaxTermCounts;
    }

    final BytesRef term = te.term();

    if (te.docFreq() > maxTermDocFreq) {
      Term t = new Term(field, term);  // this makes a deep copy of the term bytes
      TopTerm topTerm = new TopTerm();
      topTerm.term = t.bytes();
      topTerm.termNum = termNum;
      topTerm.termQuery = new TermQuery(t);

      bigTerms.put(topTerm.termNum, topTerm);

      if (deState == null) {
        deState = new SolrIndexSearcher.DocsEnumState();
        deState.fieldName = field;
        deState.liveDocs = searcher.getSlowAtomicReader().getLiveDocs();
        deState.termsEnum = te;  // TODO: check for MultiTermsEnum in SolrIndexSearcher could now fail?
        deState.postingsEnum = postingsEnum;
        deState.minSetSizeCached = maxTermDocFreq;
      }

      postingsEnum = deState.postingsEnum;
      DocSet set = searcher.getDocSet(deState);
      maxTermCounts[termNum] = set.size();
    }
  }

  @Override
  protected void setActualDocFreq(int termNum, int docFreq) {
    maxTermCounts[termNum] = docFreq;
  }

  public long memSize() {
    // can cache the mem size since it shouldn't change
    if (memsz!=0) return memsz;
    long sz = super.ramBytesUsed();
    sz += 8*8 + 32; // local fields
    sz += bigTerms.size() * 64;
    for (TopTerm tt : bigTerms.values()) {
      sz += tt.memSize();
    }
    if (maxTermCounts != null)
      sz += maxTermCounts.length * 4;
    if (indexedTermsArray != null) {
      // assume 8 byte references?
      sz += 8+8+8+8+(indexedTermsArray.length<<3)+sizeOfIndexedStrings;
    }
    memsz = sz;
    return sz;
  }

  public UnInvertedField(String field, SolrIndexSearcher searcher) throws IOException {
    super(field,
        // threshold, over which we use set intersections instead of counting
        // to (1) save memory, and (2) speed up faceting.
        // Add 2 for testing purposes so that there will always be some terms under
        // the threshold even when the index is very
        // small.
        searcher.maxDoc()/20 + 2,
        DEFAULT_INDEX_INTERVAL_BITS);

    final String prefix = TrieField.getMainValuePrefix(searcher.getSchema().getFieldType(field));
    this.searcher = searcher;
    try {
      // TODO: it's wasteful to create one of these each time
      // but DocTermOrds will throw an exception if it thinks the field has doc values (which is faked by UnInvertingReader)
      LeafReader r = SlowCompositeReaderWrapper.wrap(searcher.getRawReader());
      uninvert(r, r.getLiveDocs(), prefix == null ? null : new BytesRef(prefix));
    } catch (IllegalStateException ise) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ise);
    }
    if (tnums != null) {
      for(byte[] target : tnums) {
        if (target != null && target.length > (1<<24)*.9) {
          log.warn("Approaching too many values for UnInvertedField faceting on field '"+field+"' : bucket size=" + target.length);
        }
      }
    }

    // free space if outrageously wasteful (tradeoff memory/cpu) 
    if ((maxTermCounts.length - numTermsInField) > 1024) { // too much waste!
      int[] newMaxTermCounts = new int[numTermsInField];
      System.arraycopy(maxTermCounts, 0, newMaxTermCounts, 0, numTermsInField);
      maxTermCounts = newMaxTermCounts;
    }

    log.info("UnInverted multi-valued field " + toString());
    //System.out.println("CREATED: " + toString() + " ti.index=" + ti.index);
  }

  public int getNumTerms() {
    return numTermsInField;
  }



  public class DocToTerm implements Closeable {
    private final DocSet[] bigTermSets;
    private final int[] bigTermNums;
    private TermsEnum te;

    public DocToTerm() throws IOException {
      bigTermSets = new DocSet[bigTerms.size()];
      bigTermNums = new int[bigTerms.size()];
      int i=0;
      for (TopTerm tt : bigTerms.values()) {
        bigTermSets[i] = searcher.getDocSet(tt.termQuery);
        bigTermNums[i] = tt.termNum;
        i++;
      }
    }

    public BytesRef lookupOrd(int ord) throws IOException {
      return getTermValue( getTermsEnum() , ord );
    }

    public TermsEnum getTermsEnum() throws IOException {
      if (te == null) {
        te = getOrdTermsEnum(searcher.getSlowAtomicReader());
      }
      return te;
    }

    public void getBigTerms(int doc, Callback target) throws IOException {
      if (bigTermSets != null) {
        for (int i=0; i<bigTermSets.length; i++) {
          if (bigTermSets[i].exists(doc)) {
            target.call( bigTermNums[i] );
          }
        }
      }
    }

    public void getSmallTerms(int doc, Callback target) {
      if (termInstances > 0) {
        int code = index[doc];

        if ((code & 0xff)==1) {
          int pos = code>>>8;
          int whichArray = (doc >>> 16) & 0xff;
          byte[] arr = tnums[whichArray];
          int tnum = 0;
          for(;;) {
            int delta = 0;
            for(;;) {
              byte b = arr[pos++];
              delta = (delta << 7) | (b & 0x7f);
              if ((b & 0x80) == 0) break;
            }
            if (delta == 0) break;
            tnum += delta - TNUM_OFFSET;
            target.call(tnum);
          }
        } else {
          int tnum = 0;
          int delta = 0;
          for (;;) {
            delta = (delta << 7) | (code & 0x7f);
            if ((code & 0x80)==0) {
              if (delta==0) break;
              tnum += delta - TNUM_OFFSET;
              target.call(tnum);
              delta = 0;
            }
            code >>>= 8;
          }
        }
      }
    }

    @Override
    public void close() throws IOException {
      for (DocSet set : bigTermSets) {
        // set.decref(); // OFF-HEAP
      }
    }
  }

  public interface Callback {
    public void call(int termNum);
  }



  private void getCounts(FacetFieldProcessorByArrayUIF processor, CountSlotAcc counts) throws IOException {
    DocSet docs = processor.fcontext.base;
    int baseSize = docs.size();
    int maxDoc = searcher.maxDoc();

    // what about allBuckets?
    if (baseSize < processor.effectiveMincount) {
      return;
    }

    final int[] index = this.index;

    boolean doNegative = baseSize > maxDoc >> 1 && termInstances > 0 && docs instanceof BitDocSet;

    if (doNegative) {
      FixedBitSet bs = ((BitDocSet) docs).getBits().clone();
      bs.flip(0, maxDoc);
      // TODO: when iterator across negative elements is available, use that
      // instead of creating a new bitset and inverting.
      docs = new BitDocSet(bs, maxDoc - baseSize);
      // simply negating will mean that we have deleted docs in the set.
      // that should be OK, as their entries in our table should be empty.
    }

    // For the biggest terms, do straight set intersections
    for (TopTerm tt : bigTerms.values()) {
      // TODO: counts could be deferred if sorting by index order
      counts.incrementCount(tt.termNum, searcher.numDocs(tt.termQuery, docs));
    }

    // TODO: we could short-circuit counting altogether for sorted faceting
    // where we already have enough terms from the bigTerms

    if (termInstances > 0) {
      DocIterator iter = docs.iterator();
      while (iter.hasNext()) {
        int doc = iter.nextDoc();
        int code = index[doc];

        if ((code & 0xff) == 1) {
          int pos = code >>> 8;
          int whichArray = (doc >>> 16) & 0xff;
          byte[] arr = tnums[whichArray];
          int tnum = 0;
          for (; ; ) {
            int delta = 0;
            for (; ; ) {
              byte b = arr[pos++];
              delta = (delta << 7) | (b & 0x7f);
              if ((b & 0x80) == 0) break;
            }
            if (delta == 0) break;
            tnum += delta - TNUM_OFFSET;
            counts.incrementCount(tnum,1);
          }
        } else {
          int tnum = 0;
          int delta = 0;
          for (; ; ) {
            delta = (delta << 7) | (code & 0x7f);
            if ((code & 0x80) == 0) {
              if (delta == 0) break;
              tnum += delta - TNUM_OFFSET;
              counts.incrementCount(tnum,1);
              delta = 0;
            }
            code >>>= 8;
          }
        }
      }
    }

    if (doNegative) {
      for (int i=0; i<numTermsInField; i++) {
 //       counts[i] = maxTermCounts[i] - counts[i];
        counts.incrementCount(i, maxTermCounts[i] - counts.getCount(i)*2);
      }
    }

    /*** TODO - future optimization to handle allBuckets
    if (processor.allBucketsSlot >= 0) {
      int all = 0;  // overflow potential
      for (int i=0; i<numTermsInField; i++) {
        all += counts.getCount(i);
      }
      counts.incrementCount(processor.allBucketsSlot, all);
    }
     ***/
  }



  public void collectDocs(FacetFieldProcessorByArrayUIF processor) throws IOException {
    if (processor.collectAcc==null && processor.allBucketsAcc == null && processor.startTermIndex == 0 && processor.endTermIndex >= numTermsInField) {
      getCounts(processor, processor.countAcc);
      return;
    }

    collectDocsGeneric(processor);
  }

  // called from FieldFacetProcessor
  // TODO: do a callback version that can be specialized!
  public void collectDocsGeneric(FacetFieldProcessorByArrayUIF processor) throws IOException {
    use.incrementAndGet();

    int startTermIndex = processor.startTermIndex;
    int endTermIndex = processor.endTermIndex;
    int nTerms = processor.nTerms;
    DocSet docs = processor.fcontext.base;

    int uniqueTerms = 0;
    final CountSlotAcc countAcc = processor.countAcc;

    for (TopTerm tt : bigTerms.values()) {
      if (tt.termNum >= startTermIndex && tt.termNum < endTermIndex) {
        // handle the biggest terms
        try ( DocSet intersection = searcher.getDocSet(tt.termQuery, docs); )
        {
          int collected = processor.collectFirstPhase(intersection, tt.termNum - startTermIndex);
          countAcc.incrementCount(tt.termNum - startTermIndex, collected);
          if (collected > 0) {
            uniqueTerms++;
          }
        }
      }
    }


    if (termInstances > 0) {

      final List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
      final Iterator<LeafReaderContext> ctxIt = leaves.iterator();
      LeafReaderContext ctx = null;
      int segBase = 0;
      int segMax;
      int adjustedMax = 0;


      // TODO: handle facet.prefix here!!!

      DocIterator iter = docs.iterator();
      while (iter.hasNext()) {
        int doc = iter.nextDoc();

        if (doc >= adjustedMax) {
          do {
            ctx = ctxIt.next();
            if (ctx == null) {
              // should be impossible
              throw new RuntimeException("INTERNAL FACET ERROR");
            }
            segBase = ctx.docBase;
            segMax = ctx.reader().maxDoc();
            adjustedMax = segBase + segMax;
          } while (doc >= adjustedMax);
          assert doc >= ctx.docBase;
          processor.setNextReaderFirstPhase(ctx);
        }
        int segDoc = doc - segBase;


        int code = index[doc];

        if ((code & 0xff)==1) {
          int pos = code>>>8;
          int whichArray = (doc >>> 16) & 0xff;
          byte[] arr = tnums[whichArray];
          int tnum = 0;
          for(;;) {
            int delta = 0;
            for(;;) {
              byte b = arr[pos++];
              delta = (delta << 7) | (b & 0x7f);
              if ((b & 0x80) == 0) break;
            }
            if (delta == 0) break;
            tnum += delta - TNUM_OFFSET;
            int arrIdx = tnum - startTermIndex;
            if (arrIdx < 0) continue;
            if (arrIdx >= nTerms) break;
            countAcc.incrementCount(arrIdx, 1);
            processor.collectFirstPhase(segDoc, arrIdx);
          }
        } else {
          int tnum = 0;
          int delta = 0;
          for (;;) {
            delta = (delta << 7) | (code & 0x7f);
            if ((code & 0x80)==0) {
              if (delta==0) break;
              tnum += delta - TNUM_OFFSET;
              int arrIdx = tnum - startTermIndex;
              if (arrIdx >= 0) {
                if (arrIdx >= nTerms) break;
                countAcc.incrementCount(arrIdx, 1);
                processor.collectFirstPhase(segDoc, arrIdx);
              }
              delta = 0;
            }
            code >>>= 8;
          }
        }
      }
    }


  }



  String getReadableValue(BytesRef termval, FieldType ft, CharsRefBuilder charsRef) {
    return ft.indexedToReadable(termval, charsRef).toString();
  }

  /** may return a reused BytesRef */
  BytesRef getTermValue(TermsEnum te, int termNum) throws IOException {
    //System.out.println("getTermValue termNum=" + termNum + " this=" + this + " numTerms=" + numTermsInField);
    if (bigTerms.size() > 0) {
      // see if the term is one of our big terms.
      TopTerm tt = bigTerms.get(termNum);
      if (tt != null) {
        //System.out.println("  return big " + tt.term);
        return tt.term;
      }
    }

    return lookupTerm(te, termNum);
  }

  @Override
  public String toString() {
    final long indexSize = indexedTermsArray == null ? 0 : (8+8+8+8+(indexedTermsArray.length<<3)+sizeOfIndexedStrings); // assume 8 byte references?
    return "{field=" + field
        + ",memSize="+memSize()
        + ",tindexSize="+indexSize
        + ",time="+total_time
        + ",phase1="+phase1_time
        + ",nTerms="+numTermsInField
        + ",bigTerms="+bigTerms.size()
        + ",termInstances="+termInstances
        + ",uses="+use.get()
        + "}";
  }

  //////////////////////////////////////////////////////////////////
  //////////////////////////// caching /////////////////////////////
  //////////////////////////////////////////////////////////////////

  public static UnInvertedField getUnInvertedField(String field, SolrIndexSearcher searcher) throws IOException {
    SolrCache<String,UnInvertedField> cache = searcher.getFieldValueCache();
    if (cache == null) {
      return new UnInvertedField(field, searcher);
    }
    UnInvertedField uif = null;
    Boolean doWait = false;
    synchronized (cache) {
      uif = cache.get(field);
      if (uif == null) {
        /*
         * We use this place holder object to pull the UninvertedField construction out of the sync
         * so that if many fields are accessed in a short time, the UninvertedField can be
         * built for these fields in parallel rather than sequentially.
         */
        cache.put(field, uifPlaceholder);
      } else {
        if (uif != uifPlaceholder) {
          return uif;
        }
        doWait = true; // Someone else has put the place holder in, wait for that to complete.
      }
    }
    while (doWait) {
      try {
        synchronized (cache) {
          uif = cache.get(field); // Should at least return the placeholder, NPE if not is OK.
          if (uif != uifPlaceholder) { // OK, another thread put this in the cache we should be good.
            return uif;
          }
          cache.wait();
        }
      } catch (InterruptedException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Thread interrupted in getUninvertedField.");
      }
    }

    uif = new UnInvertedField(field, searcher);
    synchronized (cache) {
      cache.put(field, uif); // Note, this cleverly replaces the placeholder.
      cache.notifyAll();
    }

    return uif;
  }

  /* *************** Sparse implementation start *******************/
  // Sparse-enabled getCounts with fallback to standard getCounts
  // The sparse code if proof-of-concept and hacked on by copying and modifying the existing count-code
  public NamedList<Integer> getCounts(
      SolrIndexSearcher searcher, DocSet baseDocs, int offset, int limit, Integer mincount, boolean missing,
      String sort, String prefix, String termList, SparseKeys sparseKeys, SparseCounterPool pool) throws IOException {
    if (!sparseKeys.sparse) { // Fallback to standard
      return termList == null ?
          getCounts(searcher, baseDocs, offset, limit, mincount, missing, sort, prefix) :
          SimpleFacets.fallbackGetListedTermCounts(searcher, null, field, termList, baseDocs);
    }

    final int baseSize = baseDocs.size();
    final int maxDoc = searcher.maxDoc();

    if (!pool.isInitialized()) {
      pool.setFieldProperties(numTermsInField + 1, getMaxEncounteredTermDocFreq(), maxDoc, termInstances);

    }

    final boolean isProbablySparse = pool.isProbablySparse(baseSize, sparseKeys);

    long sparseTotalTime = System.nanoTime();
    use.incrementAndGet();

    FieldType ft = searcher.getSchema().getFieldType(field);

    NamedList<Integer> res = new NamedList<>();  // order is important


    //System.out.println("GET COUNTS field=" + field + " baseSize=" + baseSize + " minCount=" + mincount + " maxDoc=" + maxDoc + " numTermsInField=" + numTermsInField);
    if (baseSize >= mincount) {

      TermsEnum te = getOrdTermsEnum(searcher.getSlowAtomicReader());

      UnInvertedField.CountedTerms countedTerms = countTerms(
          searcher, te, prefix, pool, baseDocs, sparseKeys, baseSize, maxDoc, isProbablySparse);
      if (termList != null) {
        //return SimpleFacets.fallbackGetListedTermCounts(searcher, field, termList, baseDocs);
        return extractSpecificCounts(countedTerms, pool, termList, countedTerms.doNegative, baseDocs);
      }

      final CharsRefBuilder builderCache = new CharsRefBuilder();

      int off=offset;
      int lim=limit>=0 ? limit : Integer.MAX_VALUE;

      if (sort.equals(FacetParams.FACET_SORT_COUNT) || sort.equals(FacetParams.FACET_SORT_COUNT_LEGACY)) {
        int maxsize = limit>0 ? offset+limit : Integer.MAX_VALUE-1;
        maxsize = Math.min(maxsize, numTermsInField);
        LongPriorityQueue queue = new LongPriorityQueue(Math.min(maxsize,1000), maxsize, Long.MIN_VALUE);

//        int min=mincount-1;  // the smallest value in the top 'N' values
        //System.out.println("START=" + startTerm + " END=" + endTerm);

        extractTopCount(countedTerms.counts, countedTerms.startTerm, countedTerms.endTerm, mincount,
            countedTerms.doNegative, queue, pool);

        // now select the right page from the results

        // if we are deep paging, we don't have to order the highest "offset" counts.
        int collectCount = Math.max(0, queue.size() - off);
        assert collectCount <= lim;

        // the start and end indexes of our list "sorted" (starting with the highest value)
        int sortedIdxStart = queue.size() - (collectCount - 1);
        int sortedIdxEnd = queue.size() + 1;
        final long[] sorted = queue.sort(collectCount);

//        final int[] indirect = counts;  // reuse the counts array for the index into the tnums array
        // TODO: Borrow this from the SparseCounterInt and ensure it is erased up to sortedIdxLength with clear
        final int[] indirect = new int[sortedIdxEnd];
        assert indirect.length >= sortedIdxEnd;

        for (int i=sortedIdxStart; i<sortedIdxEnd; i++) {
          long pair = sorted[i];
          int c = (int)(pair >>> 32);
          int tnum = Integer.MAX_VALUE - (int)pair;

          indirect[i] = i;   // store the index for indirect sorting
          sorted[i] = tnum;  // reuse the "sorted" array to store the term numbers for indirect sorting

          // add a null label for now... we'll fill it in later.
          res.add(null, c);
        }

        // now sort the indexes by the term numbers
        PrimUtils.sort(sortedIdxStart, sortedIdxEnd, indirect, new PrimUtils.IntComparator() {
          @Override
          public int compare(int a, int b) {
            return (int)sorted[a] - (int)sorted[b];
          }

          @Override
          public boolean lessThan(int a, int b) {
            return sorted[a] < sorted[b];
          }

          @Override
          public boolean equals(int a, int b) {
            return sorted[a] == sorted[b];
          }
        });

        // convert the term numbers to term values and set
        // as the label
        //System.out.println("sortStart=" + sortedIdxStart + " end=" + sortedIdxEnd);
        final long resolveTime = System.nanoTime();
        for (int i=sortedIdxStart; i<sortedIdxEnd; i++) {
          int idx = indirect[i];
          int tnum = (int)sorted[idx];
          final String label = getReadableValue(getTermValue(countedTerms.te, tnum), ft, builderCache);
          //System.out.println("  label=" + label);
          res.setName(idx - sortedIdxStart, label);
        }
        pool.incTermResolveTimeRel(resolveTime);

      } else {
        // add results in index order
        int i = countedTerms.startTerm;
        if (mincount<=0) {
          // if mincount<=0, then we won't discard any terms and we know exactly
          // where to start.
          i= countedTerms.startTerm+off;
          off=0;
        }

        final long resolveTime = System.nanoTime();
        for (; i < countedTerms.endTerm; i++) {
          int c = (int) (countedTerms.doNegative ? maxTermCounts[i] - countedTerms.counts.get(i) :
              countedTerms.counts.get(i));
          if (c<mincount || --off>=0) continue;
          if (--lim<0) break;

          final String label = getReadableValue(getTermValue(countedTerms.te, i), ft, builderCache);
          res.add(label, c);
        }
        pool.incTermResolveTimeRel(resolveTime);
      }
      pool.release(countedTerms.counts, sparseKeys);
    }


    if (missing) {
      // TODO: a faster solution for this?
      res.add(null, SimpleFacets.getFieldMissingCount(searcher, baseDocs, field));
    }

    //System.out.println("  res=" + res);

    pool.incSimpleFacetTotalTimeRel(sparseTotalTime);
    return res;
  }

  public CountedTerms countTerms(
      SolrIndexSearcher searcher, TermsEnum te, String prefix, SparseCounterPool pool,
      DocSet docs, SparseKeys sparseKeys, int baseSize, int maxDoc, boolean isProbablySparse)
      throws IOException {
    final int[] index = this.index;
    // tricky: we add more more element than we need because we will reuse this array later
    // for ordering term ords before converting to term labels.

    // TODO: Add support for threading and other counters
    final ValueCounter counts = pool.acquire(sparseKeys,
        sparseKeys.counter == SparseKeys.COUNTER_IMPL.array ||
            (sparseKeys.counter == SparseKeys.COUNTER_IMPL.auto && !pool.usePacked(sparseKeys)) ?
            SparseKeys.COUNTER_IMPL.array : SparseKeys.COUNTER_IMPL.packed);
    if (!isProbablySparse) {
      counts.disableSparseTracking();
    }
//      final int[] counts = new int[numTermsInField + 1];

    //
    // If there is prefix, find it's start and end term numbers
    //
    int startTerm = 0;
    int endTerm = numTermsInField;  // one past the end

    if (te != null && prefix != null && prefix.length() > 0) {
      final BytesRef prefixBr = new BytesRef(prefix);
      final BytesRef prefixRefEnd = new BytesRef(prefix + UnicodeUtil.BIG_TERM);
      if (te.seekCeil(prefixBr) == TermsEnum.SeekStatus.END) {
        startTerm = numTermsInField;
      } else {
        startTerm = (int) te.ord();
      }
      if (te.seekCeil(prefixRefEnd) == TermsEnum.SeekStatus.END) {
        endTerm = numTermsInField;
      } else {
        endTerm = (int) te.ord();
      }
    }

    /* **********
    // Alternative 2: get the docSet of the prefix (could take a while) and
    // then do the intersection with the baseDocSet first.
    if (prefix != null && prefix.length() > 0) {
      docs = searcher.getDocSet(new ConstantScorePrefixQuery(new Term(field, ft.toInternal(prefix))), docs);
      // The issue with this method are problems of returning 0 counts for terms w/o
      // the prefix.  We can't just filter out those terms later because it may
      // mean that we didn't collect enough terms in the queue (in the sorted case).
    }
    ***********/

    long sparseCollectTime = System.nanoTime();
    boolean doNegative = baseSize > maxDoc >> 1 && termInstances > 0
            && startTerm==0 && endTerm==numTermsInField
            && docs instanceof BitDocSet;

    if (doNegative) {
      FixedBitSet bs = ((BitDocSet)docs).getBits().clone();
      bs.flip(0, maxDoc);
      // TODO: when iterator across negative elements is available, use that
      // instead of creating a new bitset and inverting.
      docs = new BitDocSet(bs, maxDoc - baseSize);
      // simply negating will mean that we have deleted docs in the set.
      // that should be OK, as their entries in our table should be empty.
      //System.out.println("  NEG");
    }

    // For the biggest terms, do straight set intersections
    for (UnInvertedField.TopTerm tt : bigTerms.values()) {
      //System.out.println("  do big termNum=" + tt.termNum + " term=" + tt.term.utf8ToString());
      // TODO: counts could be deferred if sorted==false
      if (tt.termNum >= startTerm && tt.termNum < endTerm) {
        counts.set(tt.termNum, searcher.numDocs(new TermQuery(new Term(field, tt.term)), docs));
        //System.out.println("    count=" + counts[tt.termNum]);
      } else {
        //System.out.println("SKIP term=" + tt.termNum);
      }
    }

    // TODO: we could short-circuit counting altogether for sorted faceting
    // where we already have enough terms from the bigTerms

    // TODO: we could shrink the size of the collection array, and
    // additionally break when the termNumber got above endTerm, but
    // it would require two extra conditionals in the inner loop (although
    // they would be predictable for the non-prefix case).
    // Perhaps a different copy of the code would be warranted.

    if (termInstances > 0) {
      DocIterator iter = docs.iterator();
      while (iter.hasNext()) {
        int doc = iter.nextDoc();
        //System.out.println("iter doc=" + doc);
        int code = index[doc];

        if ((code & 0xff)==1) {
          //System.out.println("  ptr");
          int pos = code>>>8;
          int whichArray = (doc >>> 16) & 0xff;
          byte[] arr = tnums[whichArray];
          int tnum = 0;
          for(;;) {
            int delta = 0;
            for(;;) {
              byte b = arr[pos++];
              delta = (delta << 7) | (b & 0x7f);
              if ((b & 0x80) == 0) break;
            }
            if (delta == 0) break;
            tnum += delta - TNUM_OFFSET;
            //System.out.println("    tnum=" + tnum);
            counts.inc(tnum);
          }
        } else {
          //System.out.println("  inlined");
          int tnum = 0;
          int delta = 0;
          for (;;) {
            delta = (delta << 7) | (code & 0x7f);
            if ((code & 0x80)==0) {
              if (delta==0) break;
              tnum += delta - TNUM_OFFSET;
              //System.out.println("    tnum=" + tnum);
              counts.inc(tnum);
              delta = 0;
            }
            code >>>= 8;
          }
        }
      }
    }
    pool.incCollectTimeRel(sparseCollectTime);
    return new CountedTerms(counts, startTerm, endTerm, doNegative, te);
  }

  /*
   * Sparse: Returns the raw term counts. Used by second phase of distributed faceting.
   */
/*  public CountedTerms getCountsIfSparse(
      SolrIndexSearcher searcher, DocSet baseDocs, Integer mincount, String prefix, SparseKeys sparseKeys,
      SparseCounterPool pool) throws IOException {
    if (!sparseKeys.sparse) {
      return null;
    }

    final int baseSize = baseDocs.size();

    final boolean probablyWithinCutoff = isProbablyWithinCutoff(mincount, baseSize, sparseKeys);
    if (!probablyWithinCutoff && sparseKeys.fallbackToBase) { // Fallback to standard
      return null;
    }

    pool.incSparseCalls();

    return baseSize >= mincount ?
        countTerms(searcher, prefix, pool, baseDocs, sparseKeys, baseSize, searcher.maxDoc(), probablyWithinCutoff) :
        null;
  }*/

  private NamedList<Integer> extractSpecificCounts(
      CountedTerms countedTerms, SparseCounterPool pool, String termList, boolean doNegative, DocSet docs)
      throws IOException {
    final long startTime = System.nanoTime();
    int existingTerms = 0;

    FieldType ft = searcher.getSchema().getFieldType(field);
    List<String> terms = StrUtils.splitSmart(termList, ",", true);
    NamedList<Integer> res = new NamedList<>();
    for (String term : terms) {
      String internal = ft.toInternal(term);
      long count = getTermCount(countedTerms, pool, term, new BytesRef(internal), doNegative);
      // TODO: Get existingTerms from getTermCount
      if (count == -1) {
        count = searcher.numDocs(new TermQuery(new Term(field, internal)), docs);
      }
      // Sad we have to use int
      res.add(term, (int) count);
    }
    pool.incTermsListLookupRel(termList, !terms.isEmpty() ? terms.get(terms.size()-1) : "",
        existingTerms, terms.size()-existingTerms, startTime);

    return res;
  }

  /**
   * Returns the count for the given term from the result of a previous call to {@link #countTerms}.
   *
   * @param countedTerms counts for all terms in a field.
   * @param term         the term to get counts for.
   * @param doNegative   if the counts should be subtracted from max count.
   * @return the count for the term or -1 if the term could not be located.
   * @throws IOException if the term could not be located.
   */
  public long getTermCount(CountedTerms countedTerms, SparseCounterPool pool, String origTerm, BytesRef term,
                           boolean doNegative) throws IOException {
    // TODO: Can we remove this? Aren't the bigTerms counts presented in countedTerms at this point?
    for (TopTerm tt : bigTerms.values()) {
      if (tt.term.equals(term)) {
          return doNegative ?
              maxTermCounts[tt.termNum] - countedTerms.counts.get(tt.termNum) :
              countedTerms.counts.get(tt.termNum);
      }
    }
    long index = countedTerms.te.seekExact(term) ? countedTerms.te.ord() : -1;
    if (index < 0) { // This is OK. Asking for a non-existing term is normal in distributed faceting
      // This is a missingTerm. How do we report that to stats
      return 0;
    } else if(index >= countedTerms.counts.size()) {
      System.err.println("UnInvertedField.getTermCount: ordinal for " + term + " in field " + field + " was "
          + index + " but the counts only go from 0 to ordinal " + countedTerms.counts.size());
      return -1;
    }
    return doNegative ?
        maxTermCounts[(int)index] - countedTerms.counts.get((int) index) :
        countedTerms.counts.get((int) index);
  }

  /**
   * Represents the result of a facet counter update, as performed by sparse faceting.
   */
  public static class CountedTerms {
    public final ValueCounter counts;
    public final int startTerm;
    public final int endTerm;
    public final boolean doNegative;
    public final TermsEnum te;

    public CountedTerms(ValueCounter counts, int startTerm, int endTerm, boolean doNegative, TermsEnum te) {
      this.counts = counts;
      this.startTerm = startTerm;
      this.endTerm = endTerm;
      this.doNegative = doNegative;
      this.te = te;
    }
  }

  private void extractTopCount(final ValueCounter counts, final int startTerm, final int endTerm, final int minCount,
                               final boolean doNegative, final LongPriorityQueue queue, SparseCounterPool pool) {
    long sparseExtractTime = System.nanoTime();
    if (counts.iterate(startTerm, endTerm, minCount, doNegative,
        new ValueCounter.TopCallback(maxTermCounts, minCount-1, doNegative, queue))) {
      pool.incWithinCount();
    } else {
      pool.incExceededCount();
    }
    pool.incExtractTimeRel(sparseExtractTime);
  }
  /* *************** Sparse implementation end *******************/

}
