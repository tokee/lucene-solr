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
package org.apache.solr.search.sparse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;

public class SparseKeys {
  /**
   * If true, sparse facet counting is enabled.
   */
  public static final String SPARSE = "facet.sparse";
  public static boolean SPARSE_DEFAULT = true;

  /**
   * Requests with hit count above the given boundary will be processed with vanilla Solr faceting, both for
   * phase 1 and phase 2 calls. This is a temporary property until sparse faceting for large result sets
   * has been improved to match vanilla Solr speed for same size result sets.
   * </p><p>
   * The limit can be expressed as an absolute number (integer) or a fraction of maxDoc for the full index (double).
   * </p><p>
   * Note due to rounding, this is not always disabled with 1.0. Use 2.0 or above to be sure.
   * </p><p>
   * Optional. Default is 2.0 (effectively disabling the switch).
   */
  public static final String SPARSE_BOUNDARY = "facet.sparse.boundary";
  public static final String SPARSE_BOUNDARY_DEFAULT = "2.0";

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
   * For distributed faceting, fine counting (phase 2) needs a cached & filled counter for optimal processing.
   * If such a previously filled counter is not available, one can be created or the code can fall back to vanilla
   * Solr fine count. The best choice, from a processing time point of view, depends on the size of the result set.
   * </p><p>
   * If no cached and filled counters are available, requests with hit count above the given boundary will be
   * processed with vanilla Solr fine count.
   * </p><p>
   * The limit can be expressed as an absolute number (integer) or a fraction of maxDoc for the full index (double).
   * </p><p>
   * Optional. Default is 0.5 (50% of the number of documents in the index, including deletes).
   */
  public static final String FINECOUNT_BOUNDARY = "facet.sparse.finecount.boundary";
  public static final String FINECOUNT_BOUNDARY_DEFAULT = "0.5";

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
   *   <li>dualplane: {@link org.apache.solr.search.sparse.count.DualPlaneMutable}
   *                  slow first call, performance on par with array for 50M+ counters,
   *                  lower memory usage than array.
   *                  dualplane is only effective for counters with steep long tail maxima.
   *                  It falls back to packed if insisting on dualplane would be poorer than packed.
   *                  dualplane does not support {@link #COUNTING_THREADS} > 1.</li>
   *   <li>nplane:    {@link org.apache.solr.search.sparse.count.plane.NPlaneMutable}
   *                  very slow first call, 3-5 times slower than array, extremely low memory usage.
   *                  nplane does support {@link #COUNTING_THREADS}.</li>
   *   <li>nplanez:   {@link org.apache.solr.search.sparse.count.plane.NPlaneMutable} with experimental zeroTracking.
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
   * @see {@link #HEURISTIC_SEGMENT_MINDOCS} and {@link #HEURISTIC_BOUNDARY} for setting the limit for heuristic processing.
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
  public static final String HEURISTIC_BOUNDARY = "facet.sparse.heuristic.hitsboundary";
  public static final String HEURISTIC_BOUNDARY_DEFAULT = "0.10";

  /**
   * The minimum number of documents in a segment for heuristic processing to be enabled.
   * This parameter acts as a sanity check for {@link #HEURISTIC_BOUNDARY} if that is defined as a fraction.
   * Each segment is evaluated against this number.
   *  </p><p>
   * Optional. Default is 100000.
   */
  public static final String HEURISTIC_SEGMENT_MINDOCS = "facet.sparse.heuristic.segmentmindocs";
  public static final int HEURISTIC_SEGMENT_MINDOCS_DEFAULT = 100000;

  /**
   * If {@link #HEURISTIC} is not set, this parameter has no effect.<br/>
   * If the parameter is a double (defined by containing a {@code .}), its value us multiplied with query & corpus
   * derived parameter. If the parameter is an integer, it is used directly.
   * </p></p>
   * Parameter to the formula {@code h*H + t*T + s*S + b} where<br/>
   * H = total hits in the result set.<br/>
   * T = total documents in the index.<br/>
   * S = total documents in the current segment.<br/>
   * </p><p>
   * Optional. Default is 0.0.
   */
  public static final String HEURISTIC_SAMPLE_H = "facet.sparse.heuristic.sample.h";
  public static final String HEURISTIC_SAMPLE_H_DEFAULT = "0.0";

  /**
   * If {@link #HEURISTIC} is not set, this parameter has no effect.<br/>
   * If the parameter is a double (defined by containing a {@code .}), its value us multiplied with query & corpus
   * derived parameter. If the parameter is an integer, it is used directly.
   * </p></p>
   * Parameter to the formula {@code h*H + t*T + s*S + b} where<br/>
   * H = total hits in the result set.<br/>
   * T = total documents in the index.<br/>
   * S = total documents in the current segment.<br/>
   * </p><p>
   * Optional. Default is 0.01 (1% of the total number of documents in the index).
   */
  public static final String HEURISTIC_SAMPLE_T = "facet.sparse.heuristic.sample.t";
  public static final String HEURISTIC_SAMPLE_T_DEFAULT = "0.01";

  /**
   * If {@link #HEURISTIC} is not set, this parameter has no effect.<br/>
   * If the parameter is a double (defined by containing a {@code .}), its value us multiplied with query & corpus
   * derived parameter. If the parameter is an integer, it is used directly.
   * </p></p>
   * Parameter to the formula {@code h*H + t*T + s*S + b} where<br/>
   * H = total hits in the result set.<br/>
   * T = total documents in the index.<br/>
   * S = total documents in the current segment.<br/>
   * </p><p>
   * Optional. Default is 0.1 (10% of the total number of documents in the index).
   */
  public static final String HEURISTIC_SAMPLE_S = "facet.sparse.heuristic.sample.s";
  public static final String HEURISTIC_SAMPLE_S_DEFAULT = "0.0";

  /**
   * If {@link #HEURISTIC} is not set, this parameter has no effect.<br/>
   * </p></p>
   * Parameter to the formula {@code h*H + t*T + s*S + b} where<br/>
   * H = total hits in the result set.<br/>
   * T = total documents in the index.<br/>
   * S = total documents in the current segment.<br/>
   * </p><p>
   * Optional. Default is 0.
   */
  public static final String HEURISTIC_SAMPLE_B = "facet.sparse.heuristic.sample.b";
  public static final int HEURISTIC_SAMPLE_B_DEFAULT = 0;

  /**
   * The way to perform the sampling, using the sample-size derived from {@link #HEURISTIC_SAMPLE_H},
   * {@link #HEURISTIC_SAMPLE_T}, {@link #HEURISTIC_SAMPLE_S} and {@link #HEURISTIC_SAMPLE_B}.
   * </p><p>
   * Optional. Default is 'hits'. Valid values are<br/>
   * index = sampling is performed with chunks of equal size and distribution over the full index.<br/>
   * hits = sampling is adaptive with chunks expanding to the number of documents needed to contain the wanted
   * number of hits.<br/>
   * whole = no chunking: All hits are iterated and every X is used to update the counters. Relatively slow for
   * document counts in the hundreds of millions, but is not as vulnerable to clustering as hits.
   */
  public static final String HEURISTIC_SAMPLE_MODE = "facet.sparse.heuristic.sample.mode";
  public static final String HEURISTIC_SAMPLE_MODE_DEFAULT = HEURISTIC_SAMPLE_MODES.whole.toString();
  public static enum HEURISTIC_SAMPLE_MODES {index, hits, whole}

  /**
   * The minimum sample factor used for heuristic faceting, relative to segment size or estimated segment hits.
   * If the concrete factor gets below this threshold, it will be rounded up to the threshold.
   * </p><p>
   * Optional. Default is 0.0001.
   */
  public static final String HEURISTIC_SAMPLE_MINFACTOR = "facet.sparse.heuristic.sample.minfactor";
  public static final double HEURISTIC_SAMPLE_MINFACTOR_DEFAULT = 0.0001;

  /**
   * The maximum sample factor used for heuristic faceting, relative to segment size or estimated segment hits.
   * If the concrete factor gets above this threshold, heuristics is disabled for the segment.
   * </p><p>
   * Optional. Default is 0.5.
   */
  public static final String HEURISTIC_SAMPLE_MAXFACTOR = "facet.sparse.heuristic.sample.maxfactor";
  public static final double HEURISTIC_SAMPLE_MAXFACTOR_DEFAULT = 0.5;

  /**
   * If {@link #HEURISTIC} is true, this parameter sets the number of sample chunks for each segment in the index.
   * </p><p>
   * Increasing this number increases the quality of the heuristic, at the cost of speed.
   * With a heterogeneous index with multiple segments, 1 is a fine value. With a fully optimized index a high
   * degree of document clustering, a much higher value (x1000+) might be needed.
   * </p><p>
   * Optional. Default is 10000.
   */
  public static final String HEURISTIC_SAMPLE_CHUNKS = "facet.sparse.heuristic.sample.chunks";
  public static final int HEURISTIC_SAMPLE_CHUNKS_DEFAULT = 10000;

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
   * Disable over provisioning by setting this to 1.0 and {@link #HEURISTIC_OVERPROVISION_CONSTANT} to 0.
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
   * Disable over provisioning by setting this to 0 and {@link #HEURISTIC_OVERPROVISION_FACTOR} to 1.0.
   * </p><p>
   * Optional, integer. Default is 10.
   */
  public static final String HEURISTIC_OVERPROVISION_CONSTANT = "facet.sparse.heuristic.overprovision.constant";
  public static final int HEURISTIC_OVERPROVISION_CONSTANT_DEFAULT = 10;

  public final String field;
  public final String boundary;
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
  public final String fineCountBoundary;

  /**
   * If this is non-null, the token unambigiously designates the params defining the counts for the facet.
   * This is used directly with {@link ValueCounter#getStructureKey()} for caching.
   */
  public final String cacheToken;

  public final boolean legacyShowStats;
  public final boolean resetStats;
  public final boolean cacheDistributed;

  public final boolean heuristic;
  public final int heuristicSegmentMinDocs;
  public final String heuristicBoundary;

  public final String heuristicSampleH;
  public final String heuristicSampleT;
  public final String heuristicSampleS;
  public final int heuristicSampleB;
  public final double heuristicSampleMinFactor;
  public final double heuristicSampleMaxFactor;
  public final HEURISTIC_SAMPLE_MODES heuristicSampleMode;

  public final int heuristicSampleChunks;
  public final int heuristicSampleChunksMinSize;
  public final boolean heuristicFineCount;
  public final double heuristicOverprovisionFactor;
  public final int heuristicOverprovisionConstant;

  public final String q; // Only use for logging and debugging!

  public SparseKeys(String field, SolrParams params) {
    this.field = field;

    whitelists = getRegexps(params, field, WHITELIST);
    blacklists = getRegexps(params, field, BLACKLIST);

    sparse = params.getFieldBool(field, SPARSE, SPARSE_DEFAULT);
    boundary = params.getFieldParam(field, SPARSE_BOUNDARY, SPARSE_BOUNDARY_DEFAULT);
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
    fineCountBoundary = params.getFieldParam(field, FINECOUNT_BOUNDARY, FINECOUNT_BOUNDARY_DEFAULT);

    cacheDistributed = params.getFieldBool(field, CACHE_DISTRIBUTED, CACHE_DISTRIBUTED_DEFAULT);
    cacheToken = cacheDistributed ? params.getFieldParam(field, CACHE_TOKEN, null) : null;

    legacyShowStats = params.getFieldBool(field, STATS, false);
    resetStats = params.getFieldBool(field, STATS_RESET, false);

    heuristic  = params.getFieldBool(field, HEURISTIC, HEURISTIC_DEFAULT);
    heuristicBoundary = params.getFieldParam(field, HEURISTIC_BOUNDARY, HEURISTIC_BOUNDARY_DEFAULT);
    heuristicSegmentMinDocs = params.getFieldInt(field, HEURISTIC_SEGMENT_MINDOCS, HEURISTIC_SEGMENT_MINDOCS_DEFAULT);

    heuristicSampleH = params.getFieldParam(field, HEURISTIC_SAMPLE_H, HEURISTIC_SAMPLE_H_DEFAULT);
    heuristicSampleT = params.getFieldParam(field, HEURISTIC_SAMPLE_T, HEURISTIC_SAMPLE_T_DEFAULT);
    heuristicSampleS = params.getFieldParam(field, HEURISTIC_SAMPLE_S, HEURISTIC_SAMPLE_S_DEFAULT);
    heuristicSampleB = params.getFieldInt(field, HEURISTIC_SAMPLE_B, HEURISTIC_SAMPLE_B_DEFAULT);
    heuristicSampleMinFactor = params.getFieldDouble(field,
        HEURISTIC_SAMPLE_MINFACTOR, HEURISTIC_SAMPLE_MINFACTOR_DEFAULT);
    heuristicSampleMaxFactor = params.getFieldDouble(field,
        HEURISTIC_SAMPLE_MAXFACTOR, HEURISTIC_SAMPLE_MAXFACTOR_DEFAULT);
    heuristicSampleMode = HEURISTIC_SAMPLE_MODES.valueOf(
        params.getFieldParam(field, HEURISTIC_SAMPLE_MODE, HEURISTIC_SAMPLE_MODE_DEFAULT));

    heuristicSampleChunks = params.getFieldInt(field, HEURISTIC_SAMPLE_CHUNKS, HEURISTIC_SAMPLE_CHUNKS_DEFAULT);
    heuristicSampleChunksMinSize = params.getFieldInt(field, 
        HEURISTIC_SAMPLE_CHUNKS_MINSIZE, HEURISTIC_SAMPLE_CHUNKS_MINSIZE_DEFAULT);
    heuristicFineCount = params.getFieldBool(field, HEURISTIC_FINECOUNT, HEURISTIC_FINECOUNT_DEFAULT);
    heuristicOverprovisionFactor = params.getFieldDouble(field,
        HEURISTIC_OVERPROVISION_FACTOR, HEURISTIC_OVERPROVISION_FACTOR_DEFAULT);
    heuristicOverprovisionConstant = params.getFieldInt(field,
        HEURISTIC_OVERPROVISION_CONSTANT, HEURISTIC_OVERPROVISION_CONSTANT_DEFAULT);

    q = params.get(CommonParams.Q, "");
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

  public boolean useSparse(int hitCount, int maxDoc) {
    return sparse && hitCount <= multiVal(boundary, maxDoc);
  }

  public boolean useOverallHeuristic(int hitCount, int maxDoc) {
    return heuristic && hitCount >= multiVal(heuristicBoundary, maxDoc);
  }
  public boolean useSegmentHeuristics(int indexHitCount, int indexDocuments, int segmentDocuments) {
    return segmentSampleSize(indexHitCount, indexDocuments, segmentDocuments) < segmentDocuments;
  }

  // Obeys the limits
  public int segmentSampleSize(int indexHitCount, int indexDocuments, int segmentDocuments) {
    if (!heuristic || segmentDocuments < heuristicSegmentMinDocs) {
      return segmentDocuments; // Everything
    }
    long sampleSize = segmentRawSampleSize(indexHitCount, indexDocuments, segmentDocuments);

    double factor;
    switch (heuristicSampleMode) { //
      case index: {
        factor = 1.0*sampleSize/segmentDocuments;
        if (factor < heuristicSampleMinFactor) {
          factor = heuristicSampleMinFactor;
          sampleSize = (long) (factor*segmentDocuments);
        }
        break;
      }
      case hits:
      case whole:
      { // Hits are estimated if #segments > 1
        sampleSize = Math.min(indexHitCount, sampleSize);
        double estimatedHits= indexHitCount*(1.0*segmentDocuments/indexDocuments);
        factor = sampleSize/estimatedHits;
        if (factor < heuristicSampleMinFactor) {
          factor = heuristicSampleMinFactor;
          sampleSize = (long) (factor*estimatedHits);
        }
        break;
      }
      default: throw new UnsupportedOperationException(
          "The heuristic sample mode '" + heuristicSampleMode + "' is unsupported");
    }
    if (factor > heuristicSampleMaxFactor) {
      return segmentDocuments; // Too high a factor for sampling to make sense
    }
    return (int) sampleSize;
  }
  // Derive whether the multi is a factor or a constant and either multiply or return the constant
  private double multiVal(String multi, int value) {
    return Double.parseDouble(multi) * (multi.contains(".") ? value : 1);
  }

  public int segmentRawSampleSize(int indexHitCount, int indexDocuments, int segmentDocuments) {
    long raw = (long) (multiVal(heuristicSampleH, indexHitCount) + multiVal(heuristicSampleT, indexDocuments) +
            multiVal(heuristicSampleS, segmentDocuments) + heuristicSampleB);
    return (int) Math.min(segmentDocuments, raw);
  }

  public boolean useFallbackFinecount(int hitCount, int indexMaxDoc) {
    return hitCount > multiVal(fineCountBoundary, indexMaxDoc);
  }

/*  public double segmentSampleFactor(int indexHitCount, int indexDocuments, int segmentDocuments) {
    if (fixedHeuristicSample) {
      return (int) (heuristicSampleSize.contains(".") ?
          Double.parseDouble(heuristicSampleSize) :
          1.0*Integer.parseInt(heuristicSampleSize)/segmentDocuments); // TODO: Rethink this
    }
    // See the JavaDoc for HEURISTIC_SAMPLE_F
    // a*(h/d)+c
    //double factor = heuristicSampleH*(1.0*indexHitCount/indexDocuments)+heuristicSampleB;
    double factor = heuristicSampleH*1.0*indexHitCount+heuristicSampleB;
    return Math.max(factor, heuristicSampleMinFactor);
  }*/

  @Override
  public String toString() {
    return "SparseKeys{" +
        "sparse=" + sparse +
        ", boundary=" + boundary +
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

        ", pool(" +
        "size=" + poolSize +
        ", maxCount=" + poolMaxCount +
        ", minEmpty=" + poolMinEmpty +
        ")" +

        ", skipRefinement=" + skipRefinement +
        ", fineCountBoundary=" + fineCountBoundary +
        ", cacheToken='" + cacheToken + '\'' +
        ", legacyShowStats=" + legacyShowStats +
        ", resetStats=" + resetStats +
        ", cacheDistributed=" + cacheDistributed +

        ", heuristic(" +
        "enabled=" + heuristic +
        ", minDocs=" + heuristicSegmentMinDocs +
        ", boundary=" + heuristicBoundary +

        ", sample(" +
        "mode=" + heuristicSampleMode +
        ", h=" + heuristicSampleH +
        ", t=" + heuristicSampleT +
        ", s=" + heuristicSampleS +
        ", b=" + heuristicSampleB +
        ", minFactor=" + heuristicSampleMinFactor +
        ", maxFactor=" + heuristicSampleMaxFactor +
        ", chunks=" + heuristicSampleChunks +
        ")" +

        ", fineCount=" + heuristicFineCount +

        ", overprovision(" +
        "factor=" + heuristicOverprovisionFactor +
        ", , constant=" + heuristicOverprovisionConstant +
        ")" +

        "}";
  }
}
