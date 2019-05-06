package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.CursorMark;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.RankQuery;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.eq.EqQueryCommand;

public class EqDisjunctionMaxQuery extends RankQuery implements Iterable<Query> {

    public static final int GET_SCORES = 0x01;

    private final Query[] disjuncts;

    private EqQueryCommand queryCmdWrapper;

    private EqTopFieldCollector topFieldDocCollector;

    @Override
    public Iterator<Query> iterator() {
        return getDisjuncts().iterator();
    }

    public EqDisjunctionMaxQuery(Collection<Query> disjuncts) {
        Objects.requireNonNull(disjuncts, "Collection of Queries must not be null");
        this.disjuncts = disjuncts.toArray(new Query[disjuncts.size()]);
    }

    public List<Query> getDisjuncts() {
        return Collections.unmodifiableList(Arrays.asList(disjuncts));
    }

    @Override
    public MergeStrategy getMergeStrategy() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean getCache() {
        return false;
    }

    @Override
    public RankQuery wrap(Query mainQuery) {
        return this;
    }

    @Override
    public TopDocsCollector getTopDocsCollector(int len, QueryCommand queryCommand, IndexSearcher searcher) throws IOException {
        SolrIndexSearcher solrIndexSearcher = (SolrIndexSearcher) searcher;

        SolrParams params = SolrRequestInfo.getRequestInfo().getReq().getParams();
        ResponseBuilder rb = SolrRequestInfo.getRequestInfo().getResponseBuilder();

        final CursorMark cursor = rb.getCursorMark();
        final boolean fillFields = (null != cursor);
        final FieldDoc searchAfter = (null != cursor ? cursor.getSearchAfterFieldDoc() : null);

        int maxDocRequested = rb.getSortSpec().getOffset() + rb.getSortSpec().getCount();
        // check for overflow, and check for # docs in index
        if (maxDocRequested < 0 || maxDocRequested > solrIndexSearcher.maxDoc())
            maxDocRequested = solrIndexSearcher.maxDoc();
        int supersetMaxDoc = maxDocRequested;

        final boolean needScores = (rb.getFieldFlags() & GET_SCORES) != 0;

        EqQueryCommand cmd = new EqQueryCommand(queryCommand, params, rb.req.getSchema());
        this.queryCmdWrapper = cmd;
        // operator << flow
        // if (orderByQueryIndex) {
        // sub query sorts count
        int subSortsNumber = 0;
        for (int i = 0; i < disjuncts.length; i++) {
            if (cmd.getSubQuerySorts().get("sort_" + i) != null) {
                subSortsNumber = subSortsNumber + cmd.getSubQuerySorts().get("sort_" + i).getSort().length;
            }
        }

        // global sorts
        Sort originalCommonSort = null;
        if (cmd.getOriginCmd().getSort() != null) {
            int originalSortLength = cmd.getOriginCmd().getSort().getSort().length - subSortsNumber;
            if (originalSortLength > 0) {
                originalCommonSort = new Sort(Arrays.copyOfRange(cmd.getOriginCmd().getSort().getSort(), 0, originalSortLength));
            }
        }

        for (int i = 0; i < disjuncts.length; i++) {
            // sub query sort exists for current sub query
            boolean subqSortExists = false;

            // sub query sort for current query not only SCORE DESC
            boolean subqSortNotOnlyScoreDesc = false;

            if (cmd.getSubQuerySorts().get("sort_" + i) != null) {
                subqSortExists = true;
                for (int n = 0; n < cmd.getSubQuerySorts().get("sort_" + i).getSort().length; n++) {
                    if (!(cmd.getSubQuerySorts().get("sort_" + i).getSort()[n].getField() == null
                            && cmd.getSubQuerySorts().get("sort_" + i).getSort()[n].getReverse() == false)) {
                        subqSortNotOnlyScoreDesc = true;
                    }
                }

            }

            if ((!subqSortExists && originalCommonSort != null) || (subqSortExists && !subqSortNotOnlyScoreDesc && originalCommonSort != null)) {
                cmd.getSubQuerySorts().put("sort_" + i, originalCommonSort);
            } else if ((!subqSortExists && originalCommonSort == null)
                    || (subqSortExists && !subqSortNotOnlyScoreDesc && originalCommonSort == null)) {
                cmd.getSubQuerySorts().put("sort_" + i, new Sort());
            }
        }

        // sorted array of sub query sorts
        Map<String, Sort> resultSorts = cmd.getSubQuerySorts().entrySet().stream().sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        cmd.setSubQuerySorts(resultSorts);
        Sort[] sorts = Arrays.copyOf(cmd.getSubQuerySorts().values().toArray(), cmd.getSubQuerySorts().values().toArray().length, Sort[].class);

        // filling limits by default if not exists
        for (int i = 0; i < disjuncts.length; i++) {
            if (cmd.getSubQueryLimits().get("lim_" + i) == null) {
                cmd.getSubQueryLimits().put("lim_" + i, 0);
            }
        }

        // sorted array of sub query limits
        Map<String, Integer> resultLimits = cmd.getSubQueryLimits().entrySet().stream().sorted(Map.Entry.comparingByKey())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
        cmd.setSubQueryLimits(resultLimits);
        Integer[] limits = Arrays.copyOf(cmd.getSubQueryLimits().values().toArray(), cmd.getSubQueryLimits().values().toArray().length,
                Integer[].class);
        this.topFieldDocCollector = EqTopFieldCollector.create(sorts, ArrayUtils.toPrimitive(limits), supersetMaxDoc, searchAfter, fillFields,
                needScores, needScores, true);
        return this.topFieldDocCollector;
    }

    public EqTopFieldCollector getTopFieldDocCollector() {
        return topFieldDocCollector;
    }

    public EqQueryCommand getQueryCmdWrapper() {
        return queryCmdWrapper;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new EqDisjunctionMaxWeight(searcher, scoreMode, boost);
    }

    public class EqDisjunctionMaxWeight extends Weight {

        protected final ArrayList<Weight> weights = new ArrayList<>();
        private final ScoreMode scoreMode;

        /** Construct the Weight for this Query searched by searcher.  Recursively construct subquery weights. */
        public EqDisjunctionMaxWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            super(EqDisjunctionMaxQuery.this);
            for (Query disjunctQuery : disjuncts) {
                weights.add(searcher.createWeight(disjunctQuery, scoreMode, boost));
            }
            this.scoreMode = scoreMode;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            for (Weight weight : weights) {
                weight.extractTerms(terms);
            }
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            List<Matches> mis = new ArrayList<>();
            for (Weight weight : weights) {
                Matches mi = weight.matches(context, doc);
                if (mi != null) {
                    mis.add(mi);
                }
            }
            return MatchesUtils.fromSubMatches(mis);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer[] scorers = new Scorer[weights.size()];
            boolean hasNonNullScorer = false;

            for (int i = 0; i < weights.size(); i++) {
                scorers[i] = weights.get(i).scorer(context);
                hasNonNullScorer |= (null != scorers[i]);
            }

            if (hasNonNullScorer)
                return new EqDisjunctionMaxScorer(this, scorers, scoreMode);

            return null;
        }

        static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            if (weights.size() > BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD) {
                // Disallow caching large dismax queries to not encourage users
                // to build large dismax queries as a workaround to the fact
                // that
                // we disallow caching large TermInSetQueries.
                return false;
            }
            for (Weight w : weights) {
                if (w.isCacheable(ctx) == false)
                    return false;
            }
            return false;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            boolean match = false;
            double max = 0;
            double otherSum = 0;
            List<Explanation> subs = new ArrayList<>();
            int index = 0;
            for (Weight wt : weights) {
                Explanation e = wt.explain(context, doc);
                if (!match) {
                    index++;
                }
                if (e.isMatch()) {
                    match = true;
                    subs.add(e);
                    double score = e.getValue().doubleValue();
                    if (score >= max) {
                        otherSum += max;
                        max = score;
                    } else {
                        otherSum += score;
                    }
                }
            }
            if (match) {
                final float score = (float) (max + otherSum);
                final String desc = "ordering by sub query, subQuery index: " + index + ". ";
                return Explanation.match(score, desc, subs);
            } else {
                return Explanation.noMatch("No matching clause");
            }
        }

    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {

        // if (tieBreakerMultiplier == 1.0f) {
        // BooleanQuery.Builder builder = new BooleanQuery.Builder();
        // for (Query sub : disjuncts) {
        // builder.add(sub, BooleanClause.Occur.SHOULD);
        // }
        // return builder.build();
        // }

        boolean actuallyRewritten = false;

        List<Query> rewrittenDisjuncts = new ArrayList<>();

        for (Query sub : disjuncts) {
            Query rewrittenSub = sub.rewrite(reader);
            actuallyRewritten |= rewrittenSub != sub;
            rewrittenDisjuncts.add(rewrittenSub);
        }

        if (actuallyRewritten) {
            return new EqDisjunctionMaxQuery(rewrittenDisjuncts);
        }

        return super.rewrite(reader);
    }

    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        buffer.append("(");
        for (int i = 0; i < disjuncts.length; i++) {
            Query subquery = disjuncts[i];
            if (subquery instanceof BooleanQuery) { // wrap sub-bools in parens
                buffer.append("(");
                buffer.append(subquery.toString(field));
                buffer.append(")");
            } else
                buffer.append(subquery.toString(field));
            if (i != disjuncts.length - 1)
                buffer.append(" << ");
        }
        buffer.append(")");
        // if (tieBreakerMultiplier != 0.0f) {
        // buffer.append("~");
        // buffer.append(tieBreakerMultiplier);
        // }
        return buffer.toString();
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(EqDisjunctionMaxQuery other) {
        return Arrays.equals(disjuncts, other.disjuncts);
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + Arrays.hashCode(disjuncts);
        return h;
    }

}
