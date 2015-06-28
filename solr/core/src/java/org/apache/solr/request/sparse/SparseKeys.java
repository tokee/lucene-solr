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
package org.apache.solr.request.sparse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.solr.common.params.SolrParams;

public class SparseKeys {
  /**
   * If true, sparse facet counting is enabled.
   */
  public static final String SPARSE = "facet.sparse";
  public static boolean SPARSE_DEFAULT = true;

  /**
   * If defined, facet terms matching the regexp are not considered candidates for the facet result.
   * </p><p>
   * Optional. Normally only used at search-time. Regexp. Can be defined multiple times.
   * Example: {@code myyear_198[0-9]} removes all year-terms from the eighties.
   */
  public static final String BLACKLIST = "facet.sparse.blacklist";

  /**
   * If defined, only facet terms matching the regexp are considered candidates for the facet result.
   * </p><p>
   * Optional. Normally only used at search-time. Regexp. Can be defined multiple times.
   * Example: {@code myyear_198[0-9]} keeps only the year-terms from the eighties.
   */
  public static final String WHITELIST = "facet.sparse.whitelist";

  /**
   * If true, sparse facet term lookup is enabled (if SPARSE == true). Term lookup is used by the second phase in
   * distributed faceting and is normally performed like facet.method=enum. With this option enabled, faceting calls
   * that qualifies as sparse will use the sparse implementation for resolving counts for terms.
   * </p><p>
   * Seems to work as of 20140909 and the whole sparse-thing is experimental anyway, so default is true.
   */
  public static final String TERMLOOKUP = "facet.sparse.termlookup";
  public static boolean TERMLOOKUP_DEFAULT = true;

  /**
   * If the number of unique terms in the field is <= this, all terms in the field will be cached for faster lookup.
   * This imposes a startup / index-changed penalty but makes ordinal->String resolving extremely fast as it will
   * be a direct array lookup instead of global->local ordinal resolving and potential index access.
   * </p><p>
   * Optional. Default is 0 (disabled).
   */
  public static final String TERMLOOKUP_MAXCACHE = "facet.sparse.termlookup.maxcache";
  public static int TERMLOOKUP_MAXCACHE_DEFAULT = 0;


  /**
   * The minimum number of tags in a sparse counter. If there are less tags than this, sparse will be disabled for
   * that part.
   */
  public static final String MINTAGS = "facet.sparse.mintags";
  public static int MINTAGS_DEFAULT = 10*1000;

  /**
   * Only valid with distributed sparse faceting (facet.sparse == true and multiple shards). This parameter defines
   * the maximum value used for minCount when issuing the first phase calls to the shards.
   * </p><p>
   * If minCount == 0, the shards spend a bit of extra time resolving facet values that might not be needed.
   * However, returning 0-count values might avoid a second call to the shard. This is always true if all shards has
   * the same values in the facet field and if the number of unique values is below {@code initialLimit*1.5+10}.
   * The chances of second-call avoidance falls when the number of unique values rises.
   * Consequently using minCount == 0 should be avoided for medium- to high-cardinality fields.
   * Rule of thumb: Don't use minCount == 0 with more than 100 unique values in the field.
   * </p><p>
   * Sane values are 0 or 1. Default is 1.
   */
  public static final String MAXMINCOUNT = "facet.sparse.maxmincount";
  public static final int MAXMINCOUNT_DEFAULT = 1;

  /**
   * If true, the secondary refinement phase of distributed faceting will be skipped.
   * This speeds up distributed faceting, but removes the guaranteed correct facet term counts.
   */
  public static final String SKIPREFINEMENTS = "facet.sparse.skiprefinements";
  public static final boolean SKIPREFINEMENTS_DEFAULT = false;

  /**
   * If specified (not -1), this is the maximum number any facet term counter will reach for a single shard.
   * Facet terms with a count exceeding this will still be returned and for distributed search, the total
   * count might exceed this.
   * </p><p>
   * This parameter is a performance enhancer. Setting this to a low (relative to the maximum count for any given term)
   * value makes facet counting slightly faster for standard sparse faceting. With sparse and packed faceting, this
   * value influences the amount of memory allocated for the counter structures, resulting in better performance as well
   * as lower memory overhead per facet call. Upon creation of the packed value, the actual max term count is checked
   * for the facet in order to avoid over-allocation. Setting the maxtracked is thus always a safe operation, from a
   * pure performance viewpoint.
   * </p><p>
   * Recommended values are 2^n-1, with 2^8-1 (255) and 2^16-1 (65535) being fastest.
   * </p><p>
   * Important: Setting this value means that the facet term counts might be too low and that the top-X facet terms
   * has a chance of not being the correct ones. Only enable this if the consequences are understood.
   */
  public static final String MAXTRACKED = "facet.sparse.maxtracked";
  public static int MAXTRACKED_DEFAULT = -1;

  /**
   * The size of the sparse tracker, relative to the total amount of unique tags in the facet.
   */
  public static final String FRACTION = "facet.sparse.fraction";
  public static double FRACTION_DEFAULT = 0.08; // 8%

  /**
   * If the <em>estimated number</em> (based on hitcount) of unique tags in the search result exceeds this fraction
   * of the sparse tracker, do not perform sparse tracking. The estimate is based on the assumption that references
   * from documents to tags are distributed randomly.
   */
  public static final String CUTOFF = "facet.sparse.cutoff";
  public static double CUTOFF_DEFAULT = 0.90; // 90%

  // TODO: Extended auto with overhead for dual
  /**
   * The implementation used for counting. This has significant performance- and memory-impact.
   * Valid counters are:<br/>
   * <ul>
   *   <li>auto:      Automatically selects between array and packed, based on maximum counter size, total number
   *                  of counters and threading. Will never choose dualplane or nplane.
   *                  Same first call penalty as packed</li>
   *   <li>array:     {@link org.apache.lucene.util.packed.Direct32}
   *                  fast first call, high performance, high memory.
   *                  array does not support {@link #COUNTING_THREADS}.</li>
   *   <li>packed:    {@link org.apache.lucene.util.packed.Packed64}
   *                  a bit slower first call than array, performance varying with concrete corpus and normally somewhat
   *                  below array, lower memory usage than array.
   *                  if {@link #COUNTING_THREADS} is > 1, a thread-safe packed structure is used.</li>
   *   <li>dualplane: {@link org.apache.lucene.util.packed.DualPlaneMutable}
   *                  slow first call, performance on par with array for 50M+ counters,
   *                  lower memory usage than array.
   *                  dualplane is only effective for counters with steep long tail maxima.
   *                  It falls back to packed if insisting on dualplane would be poorer than packed.
   *                  dualplane does not support {@link #COUNTING_THREADS} > 1.</li>
   *   <li>nplane:    {@link org.apache.lucene.util.packed.NPlaneMutable}
   *                  very slow first call, 3-5 times slower than array, extremely low memory usage.
   *                  nplane does support {@link #COUNTING_THREADS}.</li>
   *   <li>nplanez:   {@link org.apache.lucene.util.packed.NPlaneMutable} with experimental zeroTracking.
   *                  very slow first call, should be faster than nplane, extremely low memory usage.
   *                  nplanez does support {@link #COUNTING_THREADS}.</li>
   * </ul>
   * </p><p>
   * Optional. Default value is auto.
   */
  public static final String COUNTER = "facet.sparse.counter";
  public static final String DEFAULT_COUNTER = COUNTER_IMPL.auto.toString();

  public enum COUNTER_IMPL {auto, array, packed, dualplane, nplane, nplanez}

  /**
   * If true and the {@link #PACKED_BITLIMIT} holds, use {@link SparseCounterPacked} for counting.<br/>
   * If false, all sparse counters will be {@link SparseCounterInt}.<br/>
   * {@link #COUNTER} takes precedence over PACKED.
   * </p><p>
   * @deprecated use {@link #COUNTER} instead.
   */
  public static final String PACKED = "facet.sparse.packed";
  public static boolean DEFAULT_PACKED = true;

  /**
   * If {@link #PACKED} is true or {@link #COUNTER} is auto, counters where the maximum value of any counter is
   * <= 2^PACKED_BITLIMIT will be represented with a {@link SparseCounterPacked}.
   */
  public static final String PACKED_BITLIMIT = "facet.sparse.packed.bitlimit";
  public static int DEFAULT_PACKED_BITLIMIT = 24;

  // TODO: Fine grained config for nplane

  /**
   * If the {@link #COUNTER} supports it, the counting phase for faceting is done with the specified
   * number of threads. This increases speed at the cost of extra CPU power.
   * </p><p>
   * Optional. Default is 1.
   */
  // TODO: Consider if these should be taken from a super-pool for the facet or even for the full searcher
  public static final String COUNTING_THREADS = "facet.sparse.counting.threads";
  public static final int DEFAULT_COUNTING_THREADS = 1;
  /**
   * If the number of documents in a segment gets below this number, counting threading is not performed.
   * </p><p>
   * Optional. Default is 10000.
   */
  public static final String COUNTING_THREADS_MINDOCS = "facet.sparse.counting.threads.mindocs";
  public static final int DEFAULT_COUNTING_THREADS_MINDOCS = 10000;

  /**
   * Setting this parameter to true will add a special tag with statistics. Only for patch testing!
   * Note: The statistics are delayed when performing distributed faceting. They show the state from the previous call.
   * </p><p>
   * This parameter has been deprecated. Specify {@code debug=timing} instead.
   */
  @Deprecated
  public static final String STATS = "facet.sparse.stats";
  /**
   * Setting this to true resets collected statistics.
   */
  public static final String STATS_RESET = "facet.sparse.stats.reset";

  /**
   * If true, detailed performance statistics will be logged at INFO level in the Solr log.
   * </p><p>
   * TODO: Remove this option and the corresponding code when sparse faceting is stabilized
   * </p><p>
   * Optional. Default is false;
   */
  public static final String LOG_EXTENDED = "facet.sparse.log.extended";
  public static final boolean LOG_EXTENDED_DEFAULT = false;

  /**
   * The maximum amount of pools to hold in the {@link org.apache.solr.request.sparse.SparseCounterPoolController}.
   * Each pool is associated with an unique field in the index.
   * Optional. Default is unlimited.
   */
  public static final String POOL_MAX_COUNT = "facet.sparse.pools.max";
  public static int POOL_MAX_COUNT_DEFAULT = Integer.MAX_VALUE;

  /**
   * Maximum number of counters to store for re-use for each field.
   * Optional. Default is 2. Setting this to 0 disables re-use.
   */
  public static final String POOL_SIZE = "facet.sparse.pool.size";
  public static int POOL_SIZE_DEFAULT = 2;

  /**
   * The ideal minimum of empty counters in the pool. If the content drops below this limit, the pool might clear
   * existing filled counters or allocate new ones.
   * See {@link org.apache.solr.request.sparse.SparseCounterPool#reduceAndReturnPool()} for details.
   * </p><p>
   * Optional. Default is 1.
   */
  public static final String POOL_MIN_EMPTY = "facet.sparse.pool.minempty";
  public static int POOL_MIN_EMPTY_DEFAULT = 1;

  /**
   * The number of background threads used for cleaning used counterf for re-use.
   * </p><p>
   * Optional. Default is 1. Set to 0 to disable background clearing.
   */
  public static final String POOL_CLEANUP_THREADS = "facet.sparse.pool.cleanup.threads";
  public static final int POOL_CLEANUP_THREADS_DEFAULT = 1;

  /**
   * If true, the facet counts from phase 1 of distributed calls are cached for re-use with phase-2.
   * If the setup is single-shard, this will have no effect.
   * </p><p>
   * Optional. Default is true.
   */
  public static final String CACHE_DISTRIBUTED = "facet.sparse.cache.distributed";
  public static final boolean CACHE_DISTRIBUTED_DEFAULT = true;

  /**
   * If defined, the current request is part of distributed faceting.
   * The cachetoken uniquely defines the bitset used for filling the counters and
   * can be used as key when caching the counts.
   */
  public static final String CACHE_TOKEN = "facet.sparse.cachetoken";

  /**
   * Whether or not the calculation of the top-X terms should be done heuristically or not for large result sets.
   * @see {@link #HEURISTIC_SEGMENT_MINDOCS} and {@link #HEURISTIC_FRACTION} for setting the limit for heuristic processing.
   * </p><p>
   * Optional. Default is false.
   */
  public static final String HEURISTIC = "facet.sparse.heuristic";
  public static final boolean HEURISTIC_DEFAULT = false;

  /**
   * The point after which the top-X terms are determined using sampling, if {@link #HEURISTIC} is true.
   * This point is evaluated relative to the hitCount for the result set (and maxDoc for the index in case of fraction).
   * This can be an absolute number of hits (e.g. the integer 1000000)
   * or a fraction of documents in the shard (e.g. the double 0.10).
   * </p><p>
   * Optional. Default is 0.10.
   */
  public static final String HEURISTIC_FRACTION = "facet.sparse.heuristic.fraction";
  public static final String HEURISTIC_FRACTION_DEFAULT = "0.10";

  /**
   * The minimum number of documents in a segment for heuristic processing to be enabled.
   * This parameter acts as a sanity check for {@link #HEURISTIC_FRACTION} if that is defined as a fraction.
   * Each segment is evaluated against this number.
   *  </p><p>
   * Optional. Default is 100000.
   */
  public static final String HEURISTIC_SEGMENT_MINDOCS = "facet.sparse.heuristic.segmentmindocs";
  public static final int HEURISTIC_SEGMENT_MINDOCS_DEFAULT = 100000;

  /**
   * If {@link #HEURISTIC} is not set, this parameter has no effect.
   * If this parameter is set, it overrides {@link #HEURISTIC_SAMPLE_A} and {@link #HEURISTIC_SAMPLE_B}.
   * </p></p>
   * This size is evaluated relative to segment document count and is independent of result set size.
   * This can be an absolute number of hits (e.g. the integer 1000000)
   * or a fraction of maxDoc (e.g. the double 0.10).
   * </p><p>
   * Optional. Default is the value of {@link #HEURISTIC_FRACTION} if {@link #HEURISTIC_SAMPLE_A} and
   * {@link #HEURISTIC_SAMPLE_B} are not set.
   */
  public static final String HEURISTIC_SAMPLE_SIZE = "facet.sparse.heuristic.sample.size";

  /**
   * If {@link #HEURISTIC} is not set, this parameter has no effect.
   * If {@link #HEURISTIC_SAMPLE_SIZE} is set, this parameter has no effect.
   * </p></p>
   * This represents how large a part of the full segment document set should be used as a sample for heuristic
   * faceting. The factor is calculated with the formula {@code a*(h/d)+c}, where h = hits in the result set,
   * d=documents in the segment, a is this argument and b is {@link #HEURISTIC_SAMPLE_B}.
   * </p><p>
   * Optional. Recommended values are -1 to -20.
   */
  public static final String HEURISTIC_SAMPLE_A = "facet.sparse.heuristic.sample.a";

  /**
   * If {@link #HEURISTIC} is not set, this parameter has no effect.
   * If {@link #HEURISTIC_SAMPLE_SIZE} is set, this parameter has no effect.
   * </p></p>
   * This represents how large a part of the full segment document set should be used as a sample for heuristic
   * faceting. The factor is calculated with the formula {@code a*(h/d)+c}, where h = hits in the result set,
   * d=documents in the segment, a is {@link #HEURISTIC_SAMPLE_A} and b is this argument.
   * </p><p>
   * Optional. Default is 0.5.
   */
  public static final String HEURISTIC_SAMPLE_B = "facet.sparse.heuristic.sample.b";
  public static final double HEURISTIC_SAMPLE_B_DEFAULT = 0.5;

  /**
   * The minimum sample factor used for heuristic faceting.
   * Only takes effect if {@link #HEURISTIC_SAMPLE_A} is defined.
   * If the concrete factor gets below this threshold, it will be rounded up to the threshold.
   * </p><p>
   * Optional. Default is 1.0.
   */
  public static final String HEURISTIC_SAMPLE_MINFACTOR = "facet.sparse.heuristic.sample.minfactor";
  public static final double HEURISTIC_SAMPLE_MINFACTOR_DEFAULT = 0.001;

  /**
   * The maximum sample factor allowed for heuristic faceting.
   * Only takes effect if {@link #HEURISTIC_SAMPLE_A} is defined.
   * </p><p>
   * Optional. Default is 0.5.
   */
  public static final String HEURISTIC_SAMPLE_MAXFACTOR = "facet.sparse.heuristic.sample.maxfactor";
  public static final double HEURISTIC_SAMPLE_MAXFACTOR_DEFAULT = 0.5;

  /**
   * If {@link #HEURISTIC} is true, this parameter sets the number of sample chunks for each segment in the index.
   * Each chunk will be {@link #HEURISTIC_SAMPLE_SIZE}/HEURISTIC_SAMPLE_CHUNKS is size.
   * </p><p>
   * Increasing this number increases the quality of the heuristic, at the cost of speed.
   * With a heterogeneous index with multiple segments, 1 is a fine value. With a fully optimized index a high
   * degree of document clustering, a higher value is needed.
   * </p><p>
   * Optional. Default is 10.
   */
  public static final String HEURISTIC_SAMPLE_CHUNKS = "facet.sparse.heuristic.sample.chunks";
  public static final int HEURISTIC_SAMPLE_CHUNKS_DEFAULT = 10;

  /**
   * If {@link #HEURISTIC} is true, this parameter sets the minimum size of chunks.
   * </p><p>
   * Optional. Default is 1.
   */
  public static final String HEURISTIC_SAMPLE_CHUNKS_MINSIZE = "facet.sparse.heuristic.sample.chunks.minsize";
  public static final int HEURISTIC_SAMPLE_CHUNKS_MINSIZE_DEFAULT = 1;

  /**
   * If {@link #HEURISTIC} is true, this parameter determines if the counts for the heuristically calculated
   * top-X terms should be exact (true) or approximate (false).
   * </p><p>
   * Optional. Default is true.
   */
  public static final String HEURISTIC_FINECOUNT = "facet.sparse.heuristic.finecount";
  public static final boolean HEURISTIC_FINECOUNT_DEFAULT = true;

  /**
   * When requesting heuristic faceting, the facet.limit will be multiplied with this factor under the hood.
   * The number of terms in the facet result will be trimmed down to facet.limit before they are returned.
   * Increasing this option increases the probability that the terms are the right ones, but does not affect their
   * count. It also increases processing time.
   * </p><p>
   * Optional, double. Default is 2.0.
   */
  public static final String HEURISTIC_OVERPROVISION_FACTOR = "facet.sparse.heuristic.overprovision.factor";
  public static final double HEURISTIC_OVERPROVISION_FACTOR_DEFAULT = 2.0;

  /**
   * When requesting heuristic faceting, this value will be added to the facet.limit under the hood.
   * The number of terms in the facet result will be trimmed down to facet.limit before they are returned.
   * Increasing this option increases the probability that the terms are the right ones, but does not affect their
   * count. It also increases processing time.
   * </p><p>
   * Optional, integer. Default is 5.
   */
  public static final String HEURISTIC_OVERPROVISION_CONSTANT = "facet.sparse.heuristic.overprovision.constant";
  public static final int HEURISTIC_OVERPROVISION_CONSTANT_DEFAULT = 5;


  public final String field;
  public final List<Pattern> whitelists; // Never null
  public final List<Pattern> blacklists; // Never null

  public final boolean sparse;
  public final boolean termLookup;
  public final int termLookupMaxCache;
  public final int minTags;
  public final double fraction;
  public final double cutOff;
  public final long maxCountsTracked;
  public final boolean logExtended;

  public final COUNTER_IMPL counter;
  public final int countingThreads;
  public final int countingThreadsMinDocs;
  public final long packedLimit;

  public final int poolSize;
  public final int poolMaxCount;
  public final int poolMinEmpty;

  public final boolean skipRefinement;

  /**
   * If this is non-null, the token unambigiously designates the params defining the counts for the facet.
   * This is used directly with {@link ValueCounter#getStructureKey()} for caching.
   */
  public final String cacheToken;

  public final boolean legacyShowStats;
  public final boolean resetStats;
  public final boolean cacheDistributed;

  public final boolean heuristic;
  public final int heuristicMinDocs;
  public final String heuristicFraction;

  private final boolean fixedHeuristicSample;
  public final String heuristicSampleSize;
  private final double heuristicSampleA;
  private final double heuristicSampleB;
  private final double heuristicSampleMinFactor;
  private final double heuristicSampleMaxFactor;


  public final int heuristicSampleChunks;
  public final int heuristicSampleChunksMinSize;
  public final boolean heuristicFineCount;
  public final double heuristicOverprovisionFactor;
  public final int heuristicOverprovisionConstant;

  public SparseKeys(String field, SolrParams params) {
    this.field = field;

    whitelists = getRegexps(params, field, WHITELIST);
    blacklists = getRegexps(params, field, BLACKLIST);

    sparse = params.getFieldBool(field, SPARSE, SPARSE_DEFAULT);
    termLookup = params.getFieldBool(field, TERMLOOKUP, TERMLOOKUP_DEFAULT);
    termLookupMaxCache = params.getFieldInt(field, TERMLOOKUP_MAXCACHE, TERMLOOKUP_MAXCACHE_DEFAULT);
    minTags = params.getFieldInt(field, MINTAGS, MINTAGS_DEFAULT);
    fraction = params.getFieldDouble(field, FRACTION, FRACTION_DEFAULT);
    cutOff = params.getFieldDouble(field, CUTOFF, CUTOFF_DEFAULT);

    maxCountsTracked = Long.parseLong(params.getFieldParam(field, MAXTRACKED, Long.toString(MAXTRACKED_DEFAULT)));
    logExtended = params.getFieldBool(field, LOG_EXTENDED, LOG_EXTENDED_DEFAULT);

    countingThreads = params.getFieldInt(field, COUNTING_THREADS, DEFAULT_COUNTING_THREADS);
    countingThreadsMinDocs = params.getFieldInt(field, COUNTING_THREADS_MINDOCS, DEFAULT_COUNTING_THREADS_MINDOCS);
    if (params.getFieldParam(field, PACKED) != null && params.getFieldParam(field, COUNTER) == null) { // Old key
      counter = params.getFieldBool(field, PACKED, DEFAULT_PACKED) ? COUNTER_IMPL.auto : COUNTER_IMPL.array;
    } else {
      counter = COUNTER_IMPL.valueOf(params.getFieldParam(field, COUNTER, DEFAULT_COUNTER));
    }
    if (countingThreads > 1 && (counter == COUNTER_IMPL.dualplane || counter == COUNTER_IMPL.array)) {
      throw new IllegalArgumentException(String.format(Locale.ENGLISH,
          "The %s=%s only works with 1 thread, but %s=%d was requested",
          COUNTER, counter, COUNTING_THREADS, countingThreads));
    }
    packedLimit = params.getFieldInt(field, PACKED_BITLIMIT, DEFAULT_PACKED_BITLIMIT);

    poolSize = params.getFieldInt(field, POOL_SIZE, POOL_SIZE_DEFAULT);
    poolMaxCount = params.getFieldInt(field, POOL_MAX_COUNT, POOL_MAX_COUNT_DEFAULT);
    poolMinEmpty = params.getFieldInt(field, POOL_MIN_EMPTY, POOL_MIN_EMPTY_DEFAULT);

    skipRefinement = params.getFieldBool(field, SKIPREFINEMENTS, SKIPREFINEMENTS_DEFAULT);

    cacheDistributed = params.getFieldBool(field, CACHE_DISTRIBUTED, CACHE_DISTRIBUTED_DEFAULT);
    cacheToken = cacheDistributed ? params.getFieldParam(field, CACHE_TOKEN, null) : null;

    legacyShowStats = params.getFieldBool(field, STATS, false);
    resetStats = params.getFieldBool(field, STATS_RESET, false);

    heuristic  = params.getFieldBool(field, HEURISTIC, HEURISTIC_DEFAULT);
    heuristicFraction = params.getFieldParam(field, HEURISTIC_FRACTION, HEURISTIC_FRACTION_DEFAULT);
    heuristicMinDocs = params.getFieldInt(field, HEURISTIC_SEGMENT_MINDOCS, HEURISTIC_SEGMENT_MINDOCS_DEFAULT);

    // sampleSize defined or no sampleF
    String hss = params.getFieldParam(field, HEURISTIC_SAMPLE_SIZE, "");
    if (!"".equals(hss) || params.getFieldDouble(field, HEURISTIC_SAMPLE_B, -1.0) < 0) {
      fixedHeuristicSample = true;
      heuristicSampleSize = "".equals(hss) ? heuristicFraction : hss;
    } else {
      fixedHeuristicSample = false;
      heuristicSampleSize = "-1";
    }
    heuristicSampleA = params.getFieldDouble(field, HEURISTIC_SAMPLE_A, -1);
    heuristicSampleB = params.getFieldDouble(field, HEURISTIC_SAMPLE_B, HEURISTIC_SAMPLE_B_DEFAULT);
    heuristicSampleMinFactor = params.getFieldDouble(field,
        HEURISTIC_SAMPLE_MINFACTOR, HEURISTIC_SAMPLE_MINFACTOR_DEFAULT);
    heuristicSampleMaxFactor = params.getFieldDouble(field,
        HEURISTIC_SAMPLE_MAXFACTOR, HEURISTIC_SAMPLE_MAXFACTOR_DEFAULT);

    heuristicSampleChunks = params.getFieldInt(field, HEURISTIC_SAMPLE_CHUNKS, HEURISTIC_SAMPLE_CHUNKS_DEFAULT);
    heuristicSampleChunksMinSize = params.getFieldInt(field, 
        HEURISTIC_SAMPLE_CHUNKS_MINSIZE, HEURISTIC_SAMPLE_CHUNKS_MINSIZE_DEFAULT);
    heuristicFineCount = params.getFieldBool(field, HEURISTIC_FINECOUNT, HEURISTIC_FINECOUNT_DEFAULT);
    heuristicOverprovisionFactor = params.getFieldDouble(field,
        HEURISTIC_OVERPROVISION_FACTOR, HEURISTIC_OVERPROVISION_FACTOR_DEFAULT);
    heuristicOverprovisionConstant = params.getFieldInt(field,
        HEURISTIC_OVERPROVISION_CONSTANT, HEURISTIC_OVERPROVISION_CONSTANT_DEFAULT);
  }

  private List<Pattern> getRegexps(SolrParams params, String field, String key) {
    String[] regexps = params.getFieldParams(field, key);
    if (regexps == null || regexps.length == 0) {
      return Collections.emptyList();
    }
    List<Pattern> patterns = new ArrayList<>(regexps.length);
    for (String regexp: regexps) {
      patterns.add(Pattern.compile(regexp));
    }
    return patterns;
  }

  public boolean useOverallHeuristic(int hitCount, int maxDoc) {
    return heuristic && (heuristicFraction.contains(".") ?
        hitCount > Double.parseDouble(heuristicFraction) * maxDoc :
        hitCount > Long.parseLong(heuristicFraction));
  }
  public boolean useSegmentHeuristics(int indexHitCount, int indexDocuments, int segmentDocuments) {
    if (!heuristic || segmentDocuments < heuristicMinDocs) { // TODO: Add check for segmentSampleSize
      return false;
    }
    if (fixedHeuristicSample) {
      return true;
    }
    // See the JavaDoc for HEURISTIC_SAMPLE_F
    // a*(h/d)+c
    double factor = heuristicSampleA*(1.0*indexHitCount/indexDocuments)+heuristicSampleB;
    return factor <= heuristicSampleMaxFactor;
  }

  public int segmentSampleSizeOrig(int indexHitCount, int indexDocuments, int segmentDocuments) {
    if (fixedHeuristicSample) {
      return (int) (heuristicSampleSize.contains(".") ?
          Double.parseDouble(heuristicSampleSize) * segmentDocuments :
          Integer.parseInt(heuristicSampleSize));
    }
    // See the JavaDoc for HEURISTIC_SAMPLE_F
    // a*(h/d)+c
    double factor = heuristicSampleA*(1.0*indexHitCount/indexDocuments)+heuristicSampleB;
    return (int) (Math.max(factor, heuristicSampleMinFactor)*segmentDocuments);
  }

  public int segmentSampleSize(int indexHitCount, int indexDocuments, int segmentDocuments) {
    if (!heuristic || segmentDocuments < heuristicMinDocs) {
      return segmentDocuments; // Everything
    }
    if (fixedHeuristicSample) {
      return (int) (heuristicSampleSize.contains(".") ?
          Double.parseDouble(heuristicSampleSize) * segmentDocuments : // Fraction
          Integer.parseInt(heuristicSampleSize)*1.0*segmentDocuments/indexDocuments); // Absolute
    }
    return (int) (segmentSampleFactor(indexHitCount, indexDocuments, segmentDocuments)*segmentDocuments);
  }

  public double segmentSampleFactor(int indexHitCount, int indexDocuments, int segmentDocuments) {
    if (fixedHeuristicSample) {
      return (int) (heuristicSampleSize.contains(".") ?
          Double.parseDouble(heuristicSampleSize) :
          1.0*Integer.parseInt(heuristicSampleSize)/segmentDocuments); // TODO: Rethink this
    }
    // See the JavaDoc for HEURISTIC_SAMPLE_F
    // a*(h/d)+c
    //double factor = heuristicSampleA*(1.0*indexHitCount/indexDocuments)+heuristicSampleB;
    double factor = heuristicSampleA*1.0*indexHitCount+heuristicSampleB;
    return Math.max(factor, heuristicSampleMinFactor);
  }

  @Override
  public String toString() {
    return "SparseKeys{" +
        "sparse=" + sparse +
        ", field='" + field + '\'' +
        ", whitelists=" + whitelists +
        ", blacklists=" + blacklists +
        ", termLookup=" + termLookup +
        ", termLookupMaxCache=" + termLookupMaxCache +
        ", minTags=" + minTags +
        ", fraction=" + fraction +
        ", cutOff=" + cutOff +
        ", maxCountsTracked=" + maxCountsTracked +
        ", counter=" + counter +
        ", countingThreads=" + countingThreads +
        ", countingThreadsMinDocs=" + countingThreadsMinDocs +
        ", packedLimit=" + packedLimit +
        ", poolSize=" + poolSize +
        ", poolMaxCount=" + poolMaxCount +
        ", poolMinEmpty=" + poolMinEmpty +
        ", skipRefinement=" + skipRefinement +
        ", cacheToken='" + cacheToken + '\'' +
        ", legacyShowStats=" + legacyShowStats +
        ", resetStats=" + resetStats +
        ", cacheDistributed=" + cacheDistributed +
        ", heuristic(enabled=" + heuristic +
        ", minDocs=" + heuristicMinDocs +
        ", fraction=" + heuristicFraction +
        ", sample(fixedSize=" + fixedHeuristicSample +
        ", sampleSize=" + heuristicSampleSize +
        ", sampleChunks=" + heuristicSampleChunks +
        ", sampleA=" + heuristicSampleA +
        ", sampleB=" + heuristicSampleB +
        ", sampleMinFactor=" + heuristicSampleMinFactor +
        ", sampleMaxFactor=" + heuristicSampleMaxFactor +
        ")" +
        ", fineCount=" + heuristicFineCount +
        ", overprovisionFactor=" + heuristicOverprovisionFactor +
        ", overprovisionConstant=" + heuristicOverprovisionConstant +
        ")}";
  }
}
