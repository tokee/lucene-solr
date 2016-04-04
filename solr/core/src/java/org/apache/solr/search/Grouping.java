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
package org.apache.solr.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.grouping.AbstractAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.GroupDocs;
import org.apache.lucene.search.grouping.SearchGroup;
import org.apache.lucene.search.grouping.TopGroups;
import org.apache.lucene.search.grouping.function.FunctionAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.function.FunctionAllGroupsCollector;
import org.apache.lucene.search.grouping.function.FunctionFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.function.FunctionSecondPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupHeadsCollector;
import org.apache.lucene.search.grouping.term.TermAllGroupsCollector;
import org.apache.lucene.search.grouping.term.TermFirstPassGroupingCollector;
import org.apache.lucene.search.grouping.term.TermMemCollector;
import org.apache.lucene.search.grouping.term.TermSecondPassGroupingCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrFieldSource;
import org.apache.solr.search.grouping.collector.FilterCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic Solr Grouping infrastructure.
 * Warning NOT thread safe!
 *
 * @lucene.experimental
 */
public class Grouping {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrIndexSearcher searcher;
  private final SolrIndexSearcher.QueryResult qr;
  private final SolrIndexSearcher.QueryCommand cmd;
  private final List<Command> commands = new ArrayList<>();
  private final boolean main;
  private final boolean cacheSecondPassSearch;
  private final int maxDocsPercentageToCache;

  private Sort groupSort;
  private Sort withinGroupSort;
  private int limitDefault;
  private int docsPerGroupDefault;
  private int groupOffsetDefault;
  private Format defaultFormat;
  private TotalCount defaultTotalCount;

  private int maxDoc;
  private boolean needScores;
  private boolean getDocSet;
  private boolean getGroupedDocSet;
  private boolean getDocList; // doclist needed for debugging or highlighting
  private Query query;
  private DocSet filter;
  private Filter luceneFilter;
  private NamedList grouped = new SimpleOrderedMap();
  private Set<Integer> idSet = new LinkedHashSet<>();  // used for tracking unique docs when we need a doclist
  private int maxMatches;  // max number of matches from any grouping command
  private float maxScore = Float.NaN;  // max score seen in any doclist
  private boolean signalCacheWarning = false;
  private TimeLimitingCollector timeLimitingCollector;
  private final long startTime = System.nanoTime();

  public DocList mainResult;  // output if one of the grouping commands should be used as the main result.

  /**
   * @param cacheSecondPassSearch    Whether to cache the documents and scores from the first pass search for the second
   *                                 pass search.
   * @param maxDocsPercentageToCache The maximum number of documents in a percentage relative from maxdoc
   *                                 that is allowed in the cache. When this threshold is met,
   *                                 the cache is not used in the second pass search.
   */
  public Grouping(SolrIndexSearcher searcher,
                  SolrIndexSearcher.QueryResult qr,
                  SolrIndexSearcher.QueryCommand cmd,
                  boolean cacheSecondPassSearch,
                  int maxDocsPercentageToCache,
                  boolean main) {
    this.searcher = searcher;
    this.qr = qr;
    this.cmd = cmd;
    this.cacheSecondPassSearch = cacheSecondPassSearch;
    this.maxDocsPercentageToCache = maxDocsPercentageToCache;
    this.main = main;
  }

  public void add(Grouping.Command groupingCommand) {
    commands.add(groupingCommand);
  }

  /**
   * Adds a field command based on the specified field.
   * If the field is not compatible with {@link CommandField} it invokes the
   * {@link #addFunctionCommand(String, org.apache.solr.request.SolrQueryRequest)} method.
   *
   * @param field The fieldname to group by.
   */
  public void addFieldCommand(String field, SolrQueryRequest request) throws SyntaxError {
    SchemaField schemaField = searcher.getSchema().getField(field); // Throws an exception when field doesn't exist. Bad request.
    FieldType fieldType = schemaField.getType();
    ValueSource valueSource = fieldType.getValueSource(schemaField, null);
    if (!(valueSource instanceof StrFieldSource)) {
      addFunctionCommand(field, request);
      return;
    }

    Grouping.Command gc;
    // TODO: Make this a proper option
    // TODO: Relax the requirement of only relevance sorting
    if (request.getParams().getFieldBool(field, "group.memcache", false) &&
        groupSort == Sort.RELEVANCE && withinGroupSort == Sort.RELEVANCE) {
      logger.info("MemCache: Activated");
      Grouping.CommandMemField gcm = new CommandMemField();
      gcm.groupBy = field;
      gc = gcm;
    } else {
      logger.info("MemCache: Not activated");
      Grouping.CommandField gcf = new CommandField();
      gcf.groupBy = field;
      gc = gcf;
    }

    gc.withinGroupSort = withinGroupSort;
    gc.key = field;
    gc.numGroups = limitDefault;
    gc.docsPerGroup = docsPerGroupDefault;
    gc.groupOffset = groupOffsetDefault;
    gc.offset = cmd.getOffset();
    gc.groupSort = groupSort;
    gc.format = defaultFormat;
    gc.totalCount = defaultTotalCount;

    if (main) {
      gc.main = true;
      gc.format = Grouping.Format.simple;
    }

    if (gc.format == Grouping.Format.simple) {
      gc.groupOffset = 0;  // doesn't make sense
    }
    commands.add(gc);
  }

  public void addFunctionCommand(String groupByStr, SolrQueryRequest request) throws SyntaxError {
    QParser parser = QParser.getParser(groupByStr, "func", request);
    Query q = parser.getQuery();
    final Grouping.Command gc;
    if (q instanceof FunctionQuery) {
      ValueSource valueSource = ((FunctionQuery) q).getValueSource();
      if (valueSource instanceof StrFieldSource) {
        String field = ((StrFieldSource) valueSource).getField();
        CommandField commandField = new CommandField();
        commandField.groupBy = field;
        gc = commandField;
      } else {
        CommandFunc commandFunc = new CommandFunc();
        commandFunc.groupBy = valueSource;
        gc = commandFunc;
      }
    } else {
      CommandFunc commandFunc = new CommandFunc();
      commandFunc.groupBy = new QueryValueSource(q, 0.0f);
      gc = commandFunc;
    }
    gc.withinGroupSort = withinGroupSort;
    gc.key = groupByStr;
    gc.numGroups = limitDefault;
    gc.docsPerGroup = docsPerGroupDefault;
    gc.groupOffset = groupOffsetDefault;
    gc.offset = cmd.getOffset();
    gc.groupSort = groupSort;
    gc.format = defaultFormat;
    gc.totalCount = defaultTotalCount;

    if (main) {
      gc.main = true;
      gc.format = Grouping.Format.simple;
    }

    if (gc.format == Grouping.Format.simple) {
      gc.groupOffset = 0;  // doesn't make sense
    }

    commands.add(gc);
  }

  public void addQueryCommand(String groupByStr, SolrQueryRequest request) throws SyntaxError {
    QParser parser = QParser.getParser(groupByStr, null, request);
    Query gq = parser.getQuery();
    Grouping.CommandQuery gc = new CommandQuery();
    gc.query = gq;
    gc.withinGroupSort = withinGroupSort;
    gc.key = groupByStr;
    gc.numGroups = limitDefault;
    gc.docsPerGroup = docsPerGroupDefault;
    gc.groupOffset = groupOffsetDefault;

    // these two params will only be used if this is for the main result set
    gc.offset = cmd.getOffset();
    gc.numGroups = limitDefault;
    gc.format = defaultFormat;

    if (main) {
      gc.main = true;
      gc.format = Grouping.Format.simple;
    }
    if (gc.format == Grouping.Format.simple) {
      gc.docsPerGroup = gc.numGroups;  // doesn't make sense to limit to one
      gc.groupOffset = gc.offset;
    }

    commands.add(gc);
  }

  public Grouping setGroupSort(Sort groupSort) {
    this.groupSort = groupSort;
    return this;
  }

  public Grouping setWithinGroupSort(Sort withinGroupSort) {
    this.withinGroupSort = withinGroupSort;
    return this;
  }

  public Grouping setLimitDefault(int limitDefault) {
    this.limitDefault = limitDefault;
    return this;
  }

  public Grouping setDocsPerGroupDefault(int docsPerGroupDefault) {
    this.docsPerGroupDefault = docsPerGroupDefault;
    return this;
  }

  public Grouping setGroupOffsetDefault(int groupOffsetDefault) {
    this.groupOffsetDefault = groupOffsetDefault;
    return this;
  }

  public Grouping setDefaultFormat(Format defaultFormat) {
    this.defaultFormat = defaultFormat;
    return this;
  }

  public Grouping setDefaultTotalCount(TotalCount defaultTotalCount) {
    this.defaultTotalCount = defaultTotalCount;
    return this;
  }

  public Grouping setGetGroupedDocSet(boolean getGroupedDocSet) {
    this.getGroupedDocSet = getGroupedDocSet;
    return this;
  }

  public List<Command> getCommands() {
    return commands;
  }

  public void execute() throws IOException {
    vanillaExecute(); // TODO: Enable mem term here
  }

  // Unmodified (except for the method name) execute from Solr 5.5 (2016-03-09)
  public void vanillaExecute() throws IOException {
    if (commands.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Specify at least one field, function or query to group by.");
    }

    DocListAndSet out = new DocListAndSet();
    qr.setDocListAndSet(out);

    SolrIndexSearcher.ProcessedFilter pf = searcher.getProcessedFilter(cmd.getFilter(), cmd.getFilterList());
    final Filter luceneFilter = pf.filter;
    maxDoc = searcher.maxDoc();

    needScores = (cmd.getFlags() & SolrIndexSearcher.GET_SCORES) != 0;
    boolean cacheScores = false;
    // NOTE: Change this when withinGroupSort can be specified per group
    if (!needScores && !commands.isEmpty()) {
      Sort withinGroupSort = commands.get(0).withinGroupSort;
      cacheScores = withinGroupSort == null || withinGroupSort.needsScores();
    } else if (needScores) {
      cacheScores = needScores;
    }
    getDocSet = (cmd.getFlags() & SolrIndexSearcher.GET_DOCSET) != 0;
    getDocList = (cmd.getFlags() & SolrIndexSearcher.GET_DOCLIST) != 0;
    query = QueryUtils.makeQueryable(cmd.getQuery());

    for (Command cmd : commands) {
      cmd.prepare();
    }

    AbstractAllGroupHeadsCollector<?> allGroupHeadsCollector = null;
    List<Collector> collectors = new ArrayList<>(commands.size());
    for (Command cmd : commands) {
      Collector collector = cmd.createFirstPassCollector();
      if (collector != null) {
        collectors.add(collector);
      }
      if (getGroupedDocSet && allGroupHeadsCollector == null) {
        collectors.add(allGroupHeadsCollector = cmd.createAllGroupCollector());
      }
    }

    DocSetCollector setCollector = null;
    if (getDocSet && allGroupHeadsCollector == null) {
      setCollector = new DocSetCollector(maxDoc);
      collectors.add(setCollector);
    }
    Collector allCollectors = MultiCollector.wrap(collectors);

    CachingCollector cachedCollector = null;
    if (cacheSecondPassSearch && allCollectors != null) {
      int maxDocsToCache = (int) Math.round(maxDoc * (maxDocsPercentageToCache / 100.0d));
      // Only makes sense to cache if we cache more than zero.
      // Maybe we should have a minimum and a maximum, that defines the window we would like caching for.
      if (maxDocsToCache > 0) {
        allCollectors = cachedCollector = CachingCollector.create(allCollectors, cacheScores, maxDocsToCache);
      }
    }

    if (pf.postFilter != null) {
      pf.postFilter.setLastDelegate(allCollectors);
      allCollectors = pf.postFilter;
    }

    if (allCollectors != null) {
      searchWithTimeLimiter(luceneFilter, allCollectors);

      if(allCollectors instanceof DelegatingCollector) {
        ((DelegatingCollector) allCollectors).finish();
      }
    }

    if (getGroupedDocSet && allGroupHeadsCollector != null) {
      qr.setDocSet(new BitDocSet(allGroupHeadsCollector.retrieveGroupHeads(maxDoc)));
    } else if (getDocSet) {
      qr.setDocSet(setCollector.getDocSet());
    }

    collectors.clear();
    for (Command cmd : commands) {
      Collector collector = cmd.createSecondPassCollector();
      if (collector != null)
        collectors.add(collector);
    }

    if (!collectors.isEmpty()) {
      Collector secondPhaseCollectors = MultiCollector.wrap(collectors.toArray(new Collector[collectors.size()]));
      if (collectors.size() > 0) {
        if (cachedCollector != null) {
          if (cachedCollector.isCached()) {
            cachedCollector.replay(secondPhaseCollectors);
          } else {
            signalCacheWarning = true;
            logger.warn(String.format(Locale.ROOT, "The grouping cache is active, but not used because it exceeded the max cache limit of %d percent", maxDocsPercentageToCache));
            logger.warn("Please increase cache size or disable group caching.");
            searchWithTimeLimiter(luceneFilter, secondPhaseCollectors);
          }
        } else {
          if (pf.postFilter != null) {
            pf.postFilter.setLastDelegate(secondPhaseCollectors);
            secondPhaseCollectors = pf.postFilter;
          }
          searchWithTimeLimiter(luceneFilter, secondPhaseCollectors);
        }
        if (secondPhaseCollectors instanceof DelegatingCollector) {
          ((DelegatingCollector) secondPhaseCollectors).finish();
        }
      }
    }

    for (Command cmd : commands) {
      cmd.finish();
    }

    qr.groupedResults = grouped;

    if (getDocList) {
      int sz = idSet.size();
      int[] ids = new int[sz];
      int idx = 0;
      for (int val : idSet) {
        ids[idx++] = val;
      }
      qr.setDocList(new DocSlice(0, sz, ids, null, maxMatches, maxScore));
    }
    logger.info("Grouping finished in " + ((System.nanoTime()-startTime)/1000000L) + "ms");
  }

  /**
   * Invokes search with the specified filter and collector.  
   * If a time limit has been specified, wrap the collector in a TimeLimitingCollector
   */
  private void searchWithTimeLimiter(final Filter luceneFilter, Collector collector) throws IOException {
    if (cmd.getTimeAllowed() > 0) {
      if (timeLimitingCollector == null) {
        timeLimitingCollector = new TimeLimitingCollector(collector, TimeLimitingCollector.getGlobalCounter(), cmd.getTimeAllowed());
      } else {
        /*
         * This is so the same timer can be used for grouping's multiple phases.   
         * We don't want to create a new TimeLimitingCollector for each phase because that would 
         * reset the timer for each phase.  If time runs out during the first phase, the 
         * second phase should timeout quickly.
         */
        timeLimitingCollector.setCollector(collector);
      }
      collector = timeLimitingCollector;
    }
    try {
      Query q = query;
      if (luceneFilter != null) {
        q = new FilteredQuery(q, luceneFilter);
      }
      searcher.search(q, collector);
    } catch (TimeLimitingCollector.TimeExceededException | ExitableDirectoryReader.ExitingReaderException x) {
      logger.warn( "Query: " + query + "; " + x.getMessage() );
      qr.setPartialResults(true);
    }
  }

  /**
   * Returns offset + len if len equals zero or higher. Otherwise returns max.
   *
   * @param offset The offset
   * @param len    The number of documents to return
   * @param max    The number of document to return if len < 0 or if offset + len < 0
   * @return offset + len if len equals zero or higher. Otherwise returns max
   */
  int getMax(int offset, int len, int max) {
    int v = len < 0 ? max : offset + len;
    if (v < 0 || v > max) v = max;
    return v;
  }

  /**
   * Returns whether a cache warning should be send to the client.
   * The value <code>true</code> is returned when the cache is emptied because the caching limits where met, otherwise
   * <code>false</code> is returned.
   *
   * @return whether a cache warning should be send to the client
   */
  public boolean isSignalCacheWarning() {
    return signalCacheWarning;
  }

  //======================================   Inner classes =============================================================

  public static enum Format {

    /**
     * Grouped result. Each group has its own result set.
     */
    grouped,

    /**
     * Flat result. All documents of all groups are put in one list.
     */
    simple
  }

  public static enum TotalCount {
    /**
     * Computations should be based on groups.
     */
    grouped,

    /**
     * Computations should be based on plain documents, so not taking grouping into account.
     */
    ungrouped
  }

  /**
   * General group command. A group command is responsible for creating the first and second pass collectors.
   * A group command is also responsible for creating the response structure.
   * <p>
   * Note: Maybe the creating the response structure should be done in something like a ReponseBuilder???
   * Warning NOT thread save!
   */
  public abstract class Command<GROUP_VALUE_TYPE> {

    public String key;       // the name to use for this group in the response
    public Sort withinGroupSort;   // the sort of the documents *within* a single group.
    public Sort groupSort;        // the sort between groups
    public int docsPerGroup; // how many docs in each group - from "group.limit" param, default=1
    public int groupOffset;  // the offset within each group (for paging within each group)
    public int numGroups;    // how many groups - defaults to the "rows" parameter
    int actualGroupsToFind;  // How many groups should actually be found. Based on groupOffset and numGroups.
    public int offset;       // offset into the list of groups
    public Format format;
    public boolean main;     // use as the main result in simple format (grouped.main=true param)
    public TotalCount totalCount = TotalCount.ungrouped;

    TopGroups<GROUP_VALUE_TYPE> result;


    /**
     * Prepare this <code>Command</code> for execution.
     *
     * @throws IOException If I/O related errors occur
     */
    protected abstract void prepare() throws IOException;

    /**
     * Returns one or more {@link Collector} instances that are needed to perform the first pass search.
     * If multiple Collectors are returned then these wrapped in a {@link org.apache.lucene.search.MultiCollector}.
     *
     * @return one or more {@link Collector} instances that are need to perform the first pass search
     * @throws IOException If I/O related errors occur
     */
    protected abstract Collector createFirstPassCollector() throws IOException;

    /**
     * Returns zero or more {@link Collector} instances that are needed to perform the second pass search.
     * In the case when no {@link Collector} instances are created <code>null</code> is returned.
     * If multiple Collectors are returned then these wrapped in a {@link org.apache.lucene.search.MultiCollector}.
     *
     * @return zero or more {@link Collector} instances that are needed to perform the second pass search
     * @throws IOException If I/O related errors occur
     */
    protected Collector createSecondPassCollector() throws IOException {
      return null;
    }

    /**
     * Returns a collector that is able to return the most relevant document of all groups.
     * Returns <code>null</code> if the command doesn't support this type of collector.
     *
     * @return a collector that is able to return the most relevant document of all groups.
     * @throws IOException If I/O related errors occur
     */
    public AbstractAllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      return null;
    }

    /**
     * Performs any necessary post actions to prepare the response.
     *
     * @throws IOException If I/O related errors occur
     */
    protected abstract void finish() throws IOException;

    /**
     * Returns the number of matches for this <code>Command</code>.
     *
     * @return the number of matches for this <code>Command</code>
     */
    public abstract int getMatches();

    /**
     * Returns the number of groups found for this <code>Command</code>.
     * If the command doesn't support counting the groups <code>null</code> is returned.
     *
     * @return the number of groups found for this <code>Command</code>
     */
    protected Integer getNumberOfGroups() {
      return null;
    }

    protected NamedList commonResponse() {
      NamedList groupResult = new SimpleOrderedMap();
      grouped.add(key, groupResult);  // grouped={ key={

      int matches = getMatches();
      groupResult.add("matches", matches);
      if (totalCount == TotalCount.grouped) {
        Integer totalNrOfGroups = getNumberOfGroups();
        groupResult.add("ngroups", totalNrOfGroups == null ? 0 : totalNrOfGroups);
      }
      maxMatches = Math.max(maxMatches, matches);
      return groupResult;
    }

    protected DocList getDocList(GroupDocs groups) {
      int max = groups.totalHits;
      int off = groupOffset;
      int len = docsPerGroup;
      if (format == Format.simple) {
        off = offset;
        len = numGroups;
      }
      int docsToCollect = getMax(off, len, max);

      // TODO: implement a DocList impl that doesn't need to start at offset=0
      int docsCollected = Math.min(docsToCollect, groups.scoreDocs.length);

      int ids[] = new int[docsCollected];
      float[] scores = needScores ? new float[docsCollected] : null;
      for (int i = 0; i < ids.length; i++) {
        ids[i] = groups.scoreDocs[i].doc;
        if (scores != null)
          scores[i] = groups.scoreDocs[i].score;
      }

      float score = groups.maxScore;
      maxScore = maxAvoidNaN(score, maxScore);
      DocSlice docs = new DocSlice(off, Math.max(0, ids.length - off), ids, scores, groups.totalHits, score);

      if (getDocList) {
        DocIterator iter = docs.iterator();
        while (iter.hasNext())
          idSet.add(iter.nextDoc());
      }
      return docs;
    }

    protected void addDocList(NamedList rsp, GroupDocs groups) {
      rsp.add("doclist", getDocList(groups));
    }

    // Flatten the groups and get up offset + rows documents
    protected DocList createSimpleResponse() {
      GroupDocs[] groups = result != null ? result.groups : new GroupDocs[0];

      List<Integer> ids = new ArrayList<>();
      List<Float> scores = new ArrayList<>();
      int docsToGather = getMax(offset, numGroups, maxDoc);
      int docsGathered = 0;
      float maxScore = Float.NaN;

      outer:
      for (GroupDocs group : groups) {
        maxScore = maxAvoidNaN(maxScore, group.maxScore);

        for (ScoreDoc scoreDoc : group.scoreDocs) {
          if (docsGathered >= docsToGather) {
            break outer;
          }

          ids.add(scoreDoc.doc);
          scores.add(scoreDoc.score);
          docsGathered++;
        }
      }

      int len = docsGathered > offset ? docsGathered - offset : 0;
      int[] docs = ArrayUtils.toPrimitive(ids.toArray(new Integer[ids.size()]));
      float[] docScores = ArrayUtils.toPrimitive(scores.toArray(new Float[scores.size()]));
      DocSlice docSlice = new DocSlice(offset, len, docs, docScores, getMatches(), maxScore);

      if (getDocList) {
        for (int i = offset; i < docs.length; i++) {
          idSet.add(docs[i]);
        }
      }

      return docSlice;
    }

  }

  /** Differs from {@link Math#max(float, float)} in that if only one side is NaN, we return the other. */
  private float maxAvoidNaN(float valA, float valB) {
    if (Float.isNaN(valA) || valB > valA) {
      return valB;
    } else {
      return valA;
    }
  }

  /**
   * A group command for grouping on a field.
   */
  public class CommandField extends Command<BytesRef> {

    public String groupBy;
    TermFirstPassGroupingCollector firstPass;
    TermSecondPassGroupingCollector secondPass;

    TermAllGroupsCollector allGroupsCollector;

    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    Collection<SearchGroup<BytesRef>> topGroups;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      firstPass = new TermFirstPassGroupingCollector(groupBy, groupSort, actualGroupsToFind);
      return firstPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createSecondPassCollector() throws IOException {
      if (actualGroupsToFind <= 0) {
        allGroupsCollector = new TermAllGroupsCollector(groupBy);
        return totalCount == TotalCount.grouped ? allGroupsCollector : null;
      }

      topGroups = format == Format.grouped ? firstPass.getTopGroups(offset, false) : firstPass.getTopGroups(0, false);
      if (topGroups == null) {
        if (totalCount == TotalCount.grouped) {
          allGroupsCollector = new TermAllGroupsCollector(groupBy);
          fallBackCollector = new TotalHitCountCollector();
          return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
        } else {
          fallBackCollector = new TotalHitCountCollector();
          return fallBackCollector;
        }
      }

      int groupedDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupedDocsToCollect = Math.max(groupedDocsToCollect, 1);
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      secondPass = new TermSecondPassGroupingCollector(
          groupBy, topGroups, groupSort, withinGroupSort, groupedDocsToCollect, needScores, needScores, false
      );

      if (totalCount == TotalCount.grouped) {
        allGroupsCollector = new TermAllGroupsCollector(groupBy);
        return MultiCollector.wrap(secondPass, allGroupsCollector);
      } else {
        return secondPass;
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AbstractAllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      Sort sortWithinGroup = withinGroupSort != null ? withinGroupSort : Sort.RELEVANCE;
      return TermAllGroupHeadsCollector.create(groupBy, sortWithinGroup);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException {
      result = secondPass != null ? secondPass.getTopGroups(0) : null;
      if (main) {
        mainResult = createSimpleResponse();
        return;
      }

      NamedList groupResult = commonResponse();

      if (format == Format.simple) {
        groupResult.add("doclist", createSimpleResponse());
        return;
      }

      List groupList = new ArrayList();
      groupResult.add("groups", groupList);        // grouped={ key={ groups=[

      if (result == null) {
        return;
      }

      // handle case of rows=0
      if (numGroups == 0) return;

      for (GroupDocs<BytesRef> group : result.groups) {
        NamedList nl = new SimpleOrderedMap();
        groupList.add(nl);                         // grouped={ key={ groups=[ {


        // To keep the response format compatable with trunk.
        // In trunk MutableValue can convert an indexed value to its native type. E.g. string to int
        // The only option I currently see is the use the FieldType for this
        if (group.groupValue != null) {
          SchemaField schemaField = searcher.getSchema().getField(groupBy);
          FieldType fieldType = schemaField.getType();
          String readableValue = fieldType.indexedToReadable(group.groupValue.utf8ToString());
          IndexableField field = schemaField.createField(readableValue, 1.0f);
          nl.add("groupValue", fieldType.toObject(field));
        } else {
          nl.add("groupValue", null);
        }

        addDocList(nl, group);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? result.totalHitCount : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }
  }

  /**
   * Groups on field and attempts to achieve better speed than {@link CommandField}
   * at the cost of up-front initialization cost and (much) larger memory overhead.
   *
   * The core of the speed-optimization is a map from top-level docID to top-level Term-ordinal.
   * When the group size is above 1, an explicit cache of the scores is also used.
   */
  public class CommandMemField extends Command<BytesRef> {

    public String groupBy;
    private TermMemCollector onlyPass;

    private Doc2OrdMap docID2ordinal;
    private TermAllGroupsCollector allGroupsCollector;

    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    Collection<SearchGroup<BytesRef>> topGroups;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {

      long prepareStart = System.nanoTime();
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
      docID2ordinal = getDoc2OrdinalMap(groupBy);
      logger.info("MemCache: Prepared CommandMemField in " + ((System.nanoTime()-prepareStart)/1000000) + "ms");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      long allocateStart = System.nanoTime();
      onlyPass = new TermMemCollector(groupBy, actualGroupsToFind, docID2ordinal.si, docID2ordinal.doc2ord);
      logger.info("MemCache: Created TermMemCollector in " + ((System.nanoTime()-allocateStart)/1000000) + "ms");
      return onlyPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createSecondPassCollector() throws IOException {
      return null; // AllGroups if free in TermMemCollector
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException {
      long finishStart = System.nanoTime();
      //int groupedDocsToCollect = Math.max(1, getMax(groupOffset, docsPerGroup, maxDoc));

      if (main) {
        mainResult = createSimpleResponse();
        return;
      }

      NamedList groupResult = commonResponse();

      if (format == Format.simple) {
        groupResult.add("doclist", createSimpleResponse());
        return;
      }

      List groupList = new ArrayList();
      groupResult.add("groups", groupList);        // grouped={ key={ groups=[

      result = onlyPass.collectGroupDocs(groupOffset, docsPerGroup);
      if (result == null) {
        return;
      }

      // handle case of rows=0
      if (numGroups == 0) return;

      for (GroupDocs<BytesRef> group : result.groups) {
        NamedList nl = new SimpleOrderedMap();
        groupList.add(nl);                         // grouped={ key={ groups=[ {


        // To keep the response format compatable with trunk.
        // In trunk MutableValue can convert an indexed value to its native type. E.g. string to int
        // The only option I currently see is the use the FieldType for this
        if (group.groupValue != null) {
          SchemaField schemaField = searcher.getSchema().getField(groupBy);
          FieldType fieldType = schemaField.getType();
          String readableValue = fieldType.indexedToReadable(group.groupValue.utf8ToString());
          IndexableField field = schemaField.createField(readableValue, 1.0f);
          nl.add("groupValue", fieldType.toObject(field));
        } else {
          nl.add("groupValue", null);
        }

        addDocList(nl, group);
      }
      long closeTime = -System.nanoTime();
      boolean sparseClear = onlyPass.close();
      closeTime += System.nanoTime();
      logger.info("MemCache: Finish-method finished in " + ((System.nanoTime()-finishStart)/1000000) +
          "ms (" + closeTime/1000000 + "ms for " + (sparseClear ? "sparse" : "dense") + " array release)");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      if (onlyPass == null && fallBackCollector == null) {
        return 0;
      }

      return onlyPass != null ? onlyPass.getTotalHitCount() : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }

  }

  // TODO: Extremely hackish and fails on index update. Custom caching is needed (see searcher.cacheLookup et al)
  private static final Map<String, Doc2OrdMap> groupCache = new HashMap<>();
  private Doc2OrdMap getDoc2OrdinalMap(String field) throws IOException {
    final long startTime = System.currentTimeMillis();
    final String cacheKey = "grouping_doc2ord_field=" + field;

    synchronized (Grouping.class) {
      Doc2OrdMap doc2ord = groupCache.get(cacheKey);
      if (doc2ord != null) {
        return doc2ord;
      }

      logger.info("Creating top-level docID ➡ ordinal ➡ term structures for field " + field);

      // Create segment-ordinal ➡ global-ordinal mapper as well as global-ordinal ➡ term mapper
      SchemaField schemaField = searcher.getSchema().getField(field);
      FieldType ft = schemaField.getType();
      if (schemaField.multiValued() || ft.multiValuedFieldCache()) {
        // TODO: Consider supporting multi-value by taking the first value
        throw new IllegalStateException("Grouping not supported on multi valued field " + field);
      }

      final SortedSetDocValues si; // for term lookups only
      MultiDocValues.OrdinalMap ordinalMap = null; // for mapping per-segment ords to global ones

      SortedDocValues single = searcher.getLeafReader().getSortedDocValues(field);
      si = single == null ? null : DocValues.singleton(single);
      if (single instanceof MultiDocValues.MultiSortedDocValues) {
        ordinalMap = ((MultiDocValues.MultiSortedDocValues) single).mapping;
      }
      if (si == null) {
        throw new UnsupportedOperationException("Unable to determine SortedSetDocValues");
      }
      if (si.getValueCount() >= Integer.MAX_VALUE) {
        throw new UnsupportedOperationException("Cannot group on more than 2 billion unique values, sorry");
      }
      final SortedDocValues singleton = DocValues.unwrapSingleton(si);

      // Create global-docID ➡ global-ordinal mapper
      int leaveCount = searcher.getIndexReader().leaves().size();
      if (ordinalMap == null && leaveCount != 1) {
        throw new IllegalStateException( // TODO: What about 0 leaves?
            "Logic error: top-level ordinal map is null, but there are " + leaveCount + " leaves. Expected 1 leaf");
      }

      PackedInts.Mutable d2o = ordinalMap == null ?
          PackedInts.getMutable(searcher.maxDoc(), PackedInts.bitsRequired(
              searcher.getIndexReader().leaves().get(0).reader().maxDoc()
          ), PackedInts.FAST) :
          PackedInts.getMutable(searcher.maxDoc(), PackedInts.bitsRequired(ordinalMap.getValueCount()), PackedInts.FAST);
      int leafIndex = 0;
      for (LeafReaderContext leafContext : searcher.getIndexReader().leaves()) {
        int segmentMaxDoc = leafContext.reader().maxDoc();
        final LongValues segmentMap = ordinalMap == null ? null : ordinalMap.getGlobalOrds(leafIndex++);
        SortedDocValues segmentDV = leafContext.reader().getSortedDocValues(field);
        if (segmentDV == null) {
          segmentDV = DocValues.emptySorted();
        }

        for (int segmentDocID = 0; segmentDocID < segmentMaxDoc; segmentDocID++) {

          final int globalDocID = leafContext.docBase + segmentDocID;
          int segmentOrd = segmentDV.getOrd(segmentDocID);
          if (segmentMap != null && segmentOrd >= 0) {
            d2o.set(globalDocID, segmentMap.get(segmentOrd));
          } else if (ordinalMap == null && segmentOrd >= 0) {
            d2o.set(globalDocID, segmentOrd); // TODO: Single segment mapper could be created a lot faster
          }
        }
      }
      Doc2OrdMap d2om = new Doc2OrdMap(singleton, d2o);
      groupCache.put(cacheKey, d2om);
      logger.info("Created top-level docID ➡ ordinal ➡ term structures for field " + field
          + " in " + (System.currentTimeMillis() - startTime) + "ms");
      return d2om;
    }
  }
  public class Doc2OrdMap {
    private final SortedDocValues si; // global ord -> term
    private final PackedInts.Reader doc2ord; // global docID -> global ord

    public Doc2OrdMap(SortedDocValues si, PackedInts.Reader doc2ord) {
      this.si = si;
      this.doc2ord = doc2ord;
    }

    public BytesRef getTermFromDoc(int globalDocID) {
      return si.get(globalDocID);
    }

    public BytesRef getTermFromOrd(int globalOrdinal) {
      return si.lookupOrd(globalOrdinal);
    }
  }


  /**
   * A group command for grouping on a query.
   */
  //NOTE: doesn't need to be generic. Maybe Command interface --> First / Second pass abstract impl.
  public class CommandQuery extends Command {

    public Query query;
    TopDocsCollector topCollector;
    FilterCollector collector;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      DocSet groupFilt = searcher.getDocSet(query);
      topCollector = newCollector(withinGroupSort, needScores);
      collector = new FilterCollector(groupFilt, topCollector);
      return collector;
    }

    TopDocsCollector newCollector(Sort sort, boolean needScores) throws IOException {
      int groupDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      if (sort == null || sort.equals(Sort.RELEVANCE)) {
        return TopScoreDocCollector.create(groupDocsToCollect);
      } else {
        return TopFieldCollector.create(searcher.weightSort(sort), groupDocsToCollect, false, needScores, needScores);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException {
      TopDocsCollector topDocsCollector = (TopDocsCollector) collector.getDelegate();
      TopDocs topDocs = topDocsCollector.topDocs();
      GroupDocs<String> groupDocs = new GroupDocs<>(Float.NaN, topDocs.getMaxScore(), topDocs.totalHits, topDocs.scoreDocs, query.toString(), null);
      if (main) {
        mainResult = getDocList(groupDocs);
      } else {
        NamedList rsp = commonResponse();
        addDocList(rsp, groupDocs);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      return collector.getMatches();
    }
  }

  /**
   * A command for grouping on a function.
   */
  public class CommandFunc extends Command<MutableValue> {

    public ValueSource groupBy;
    Map context;

    FunctionFirstPassGroupingCollector firstPass;
    FunctionSecondPassGroupingCollector secondPass;
    // If offset falls outside the number of documents a group can provide use this collector instead of secondPass
    TotalHitCountCollector fallBackCollector;
    FunctionAllGroupsCollector allGroupsCollector;
    Collection<SearchGroup<MutableValue>> topGroups;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void prepare() throws IOException {
      context = ValueSource.newContext(searcher);
      groupBy.createWeight(context, searcher);
      actualGroupsToFind = getMax(offset, numGroups, maxDoc);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createFirstPassCollector() throws IOException {
      // Ok we don't want groups, but do want a total count
      if (actualGroupsToFind <= 0) {
        fallBackCollector = new TotalHitCountCollector();
        return fallBackCollector;
      }

      groupSort = groupSort == null ? Sort.RELEVANCE : groupSort;
      firstPass = new FunctionFirstPassGroupingCollector(groupBy, context, searcher.weightSort(groupSort), actualGroupsToFind);
      return firstPass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collector createSecondPassCollector() throws IOException {
      if (actualGroupsToFind <= 0) {
        allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
        return totalCount == TotalCount.grouped ? allGroupsCollector : null;
      }

      topGroups = format == Format.grouped ? firstPass.getTopGroups(offset, false) : firstPass.getTopGroups(0, false);
      if (topGroups == null) {
        if (totalCount == TotalCount.grouped) {
          allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
          fallBackCollector = new TotalHitCountCollector();
          return MultiCollector.wrap(allGroupsCollector, fallBackCollector);
        } else {
          fallBackCollector = new TotalHitCountCollector();
          return fallBackCollector;
        }
      }

      int groupdDocsToCollect = getMax(groupOffset, docsPerGroup, maxDoc);
      groupdDocsToCollect = Math.max(groupdDocsToCollect, 1);
      Sort withinGroupSort = this.withinGroupSort != null ? this.withinGroupSort : Sort.RELEVANCE;
      secondPass = new FunctionSecondPassGroupingCollector(
          topGroups, groupSort, withinGroupSort, groupdDocsToCollect, needScores, needScores, false, groupBy, context
      );

      if (totalCount == TotalCount.grouped) {
        allGroupsCollector = new FunctionAllGroupsCollector(groupBy, context);
        return MultiCollector.wrap(secondPass, allGroupsCollector);
      } else {
        return secondPass;
      }
    }

    @Override
    public AbstractAllGroupHeadsCollector<?> createAllGroupCollector() throws IOException {
      Sort sortWithinGroup = withinGroupSort != null ? withinGroupSort : Sort.RELEVANCE;
      return new FunctionAllGroupHeadsCollector(groupBy, context, sortWithinGroup);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void finish() throws IOException {
      result = secondPass != null ? secondPass.getTopGroups(0) : null;
      if (main) {
        mainResult = createSimpleResponse();
        return;
      }

      NamedList groupResult = commonResponse();

      if (format == Format.simple) {
        groupResult.add("doclist", createSimpleResponse());
        return;
      }

      List groupList = new ArrayList();
      groupResult.add("groups", groupList);        // grouped={ key={ groups=[

      if (result == null) {
        return;
      }

      // handle case of rows=0
      if (numGroups == 0) return;

      for (GroupDocs<MutableValue> group : result.groups) {
        NamedList nl = new SimpleOrderedMap();
        groupList.add(nl);                         // grouped={ key={ groups=[ {
        nl.add("groupValue", group.groupValue.toObject());
        addDocList(nl, group);
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMatches() {
      if (result == null && fallBackCollector == null) {
        return 0;
      }

      return result != null ? result.totalHitCount : fallBackCollector.getTotalHits();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getNumberOfGroups() {
      return allGroupsCollector == null ? null : allGroupsCollector.getGroupCount();
    }

  }

}
