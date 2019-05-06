package org.apache.solr.handler.component.eq;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.search.EqDisjunctionMaxQuery;
import org.apache.lucene.search.EqFieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.response.ResultContext;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.eq.EqBasicResultContext;
import org.apache.solr.search.eq.EqQueryCommand;
import org.apache.solr.search.eq.EqSolrReturnFields;
import org.apache.solr.search.eq.EqSortSpecParsing;

public class EqQueryComponent extends QueryComponent {

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        super.prepare(rb);
        // trying to find sub query sorts
        Integer start = rb.req.getParams().getInt(CommonParams.START);
        Integer rows = rb.req.getParams().getInt(CommonParams.ROWS);
        start = start != null ? start : CommonParams.START_DEFAULT;
        rows = rows != null ? rows : CommonParams.ROWS_DEFAULT;

        SortSpec sortSpec = EqSortSpecParsing.parseSortSpec(rb.req.getParams().get(CommonParams.SORT), rb.req);
        sortSpec.setOffset(start);
        sortSpec.setCount(rows);
        rb.setSortSpec(sortSpec);
        rb.setFieldFlags(0);
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
        super.process(rb);
        if (rb.getQuery() instanceof EqDisjunctionMaxQuery) {
            EqDisjunctionMaxQuery disjunctionMaxQuery = (EqDisjunctionMaxQuery) rb.getQuery();
            if (disjunctionMaxQuery.getTopFieldDocCollector() != null) {
                EqQueryCommand cmd = disjunctionMaxQuery.getQueryCmdWrapper();
                TopDocs topDocs = disjunctionMaxQuery.getTopFieldDocCollector().getSavedTopDocs();

                int nDocsReturned = topDocs.scoreDocs.length;
                Map<Integer, EqFieldDoc> fieldDocsMap = new HashMap<Integer, EqFieldDoc>(nDocsReturned);
                for (int i = 0; i < nDocsReturned; i++) {
                    EqFieldDoc scoreDoc = (EqFieldDoc) topDocs.scoreDocs[i];
                    fieldDocsMap.put(scoreDoc.doc, scoreDoc);
                }

                ResultContext ctx = new EqBasicResultContext(rb.getResults().docList, new EqSolrReturnFields(rb.req), rb.req.getSearcher(),
                        disjunctionMaxQuery, rb.req, fieldDocsMap);
                rb.rsp.getValues().remove("response");
                rb.rsp.add("response", ctx);
            }
        }
    }

    @Override
    protected void mergeIds(ResponseBuilder rb, ShardRequest sreq) {
        List<MergeStrategy> mergeStrategies = rb.getMergeStrategies();
        if (mergeStrategies != null) {
            Collections.sort(mergeStrategies, MergeStrategy.MERGE_COMP);
            boolean idsMerged = false;
            for (MergeStrategy mergeStrategy : mergeStrategies) {
                mergeStrategy.merge(rb, sreq);
                if (mergeStrategy.mergesIds()) {
                    idsMerged = true;
                }
            }

            if (idsMerged) {
                return; // ids were merged above so return.
            }
        }

        SortSpec ss = rb.getSortSpec();
        Sort sort = ss.getSort();

        SortField[] sortFields = null;
        if (sort != null)
            sortFields = sort.getSort();
        else {
            sortFields = new SortField[] { SortField.FIELD_SCORE };
        }

        IndexSchema schema = rb.req.getSchema();
        SchemaField uniqueKeyField = schema.getUniqueKeyField();
        Map<Integer, SortField[]> sortFieldsMap = new TreeMap<>();
        Map<Integer, Integer> limitsMap = new TreeMap<>();

        if (rb.getQuery() instanceof EqDisjunctionMaxQuery) {
            EqDisjunctionMaxQuery customDisjunctionMaxQuery = (EqDisjunctionMaxQuery) rb.getQuery();

            // operator << flow
            for (int i = 0; i < customDisjunctionMaxQuery.getDisjuncts().size(); i++) {

                // sorting
                int subqIndex = i + 1;

                // sub query sorts
                String disjunctSortParams = sreq.params.get("sort_" + i);
                // global sorts
                String commonSortParams = sreq.params.get("sort");

                // be sure current sub query sort not only SCORE DESC
                String disjunctSortParamsNoScore = null;
                if (disjunctSortParams != null) {
                    disjunctSortParamsNoScore = disjunctSortParams.replaceAll(",*\\s*score desc,*\\s*", "");
                }

                if (disjunctSortParams != null && !disjunctSortParamsNoScore.isEmpty()) {
                    sortFieldsMap.put(subqIndex, EqSortSpecParsing.parseSortSpec(disjunctSortParams, rb.req).getSort().getSort());
                } else if (commonSortParams != null && !commonSortParams.trim().equals("score desc")) {
                    sortFieldsMap.put(subqIndex, EqSortSpecParsing.parseSortSpec(commonSortParams, rb.req).getSort().getSort());
                } else {
                    sortFieldsMap.put(subqIndex, new SortField[] { SortField.FIELD_SCORE });
                }

                // limits
                if (sreq.params.get("lim_" + i) != null) {
                    limitsMap.put(subqIndex, Integer.valueOf(sreq.params.get("lim_" + i).trim()));
                } else {
                    limitsMap.put(subqIndex, 0);
                }
            }
        }

        // id to shard mapping, to eliminate any accidental dups
        HashMap<Object, String> uniqueDoc = new HashMap<>();

        // Merge the docs via a priority queue so we don't have to sort *all* of
        // the
        // documents... we only need to order the top (rows+start)
        EqShardFieldSortedHitQueue queue;
        if (sortFieldsMap.isEmpty()) {
            queue = new EqShardFieldSortedHitQueue(sortFields, ss.getOffset() + ss.getCount(), rb.req.getSearcher());
        } else {
            queue = new EqShardFieldSortedHitQueue(sortFieldsMap, ss.getOffset() + ss.getCount() * sortFieldsMap.size(), rb.req.getSearcher());
        }

        NamedList<Object> shardInfo = null;
        if (rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
            shardInfo = new SimpleOrderedMap<>();
            rb.rsp.getValues().add(ShardParams.SHARDS_INFO, shardInfo);
        }

        long numFound = 0;
        Float maxScore = null;
        boolean partialResults = false;
        Boolean segmentTerminatedEarly = null;
        for (ShardResponse srsp : sreq.responses) {
            SolrDocumentList docs = null;
            NamedList<?> responseHeader = null;

            if (shardInfo != null) {
                SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();

                if (srsp.getException() != null) {
                    Throwable t = srsp.getException();
                    if (t instanceof SolrServerException) {
                        t = ((SolrServerException) t).getCause();
                    }
                    nl.add("error", t.toString());
                    StringWriter trace = new StringWriter();
                    t.printStackTrace(new PrintWriter(trace));
                    nl.add("trace", trace.toString());
                    if (srsp.getShardAddress() != null) {
                        nl.add("shardAddress", srsp.getShardAddress());
                    }
                } else {
                    responseHeader = (NamedList<?>) srsp.getSolrResponse().getResponse().get("responseHeader");
                    final Object rhste = (responseHeader == null ? null
                            : responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY));
                    if (rhste != null) {
                        nl.add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, rhste);
                    }
                    docs = (SolrDocumentList) srsp.getSolrResponse().getResponse().get("response");
                    nl.add("numFound", docs.getNumFound());
                    nl.add("maxScore", docs.getMaxScore());
                    nl.add("shardAddress", srsp.getShardAddress());
                }
                if (srsp.getSolrResponse() != null) {
                    nl.add("time", srsp.getSolrResponse().getElapsedTime());
                }

                shardInfo.add(srsp.getShard(), nl);
            }
            // now that we've added the shard info, let's only proceed if we
            // have no error.
            if (srsp.getException() != null) {
                partialResults = true;
                continue;
            }

            if (docs == null) { // could have been initialized in the shards
                                // info block above
                docs = (SolrDocumentList) srsp.getSolrResponse().getResponse().get("response");
            }

            if (responseHeader == null) { // could have been initialized in the
                                          // shards info block above
                responseHeader = (NamedList<?>) srsp.getSolrResponse().getResponse().get("responseHeader");
            }

            if (responseHeader != null) {
                if (Boolean.TRUE.equals(responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY))) {
                    partialResults = true;
                }
                if (!Boolean.TRUE.equals(segmentTerminatedEarly)) {
                    final Object ste = responseHeader.get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
                    if (Boolean.TRUE.equals(ste)) {
                        segmentTerminatedEarly = Boolean.TRUE;
                    } else if (Boolean.FALSE.equals(ste)) {
                        segmentTerminatedEarly = Boolean.FALSE;
                    }
                }
            }

            // calculate global maxScore and numDocsFound
            if (docs.getMaxScore() != null) {
                maxScore = maxScore == null ? docs.getMaxScore() : Math.max(maxScore, docs.getMaxScore());
            }
            numFound += docs.getNumFound();

            NamedList sortFieldValues = (NamedList) (srsp.getSolrResponse().getResponse().get("sort_values"));
            NamedList unmarshalledSortFieldValues = unmarshalSortValues(ss, sortFieldValues, schema);

            // go through every doc in this response, construct a ShardDoc, and
            // put it in the priority queue so it can be ordered.
            for (int i = 0; i < docs.size(); i++) {
                SolrDocument doc = docs.get(i);
                Object id = doc.getFieldValue(uniqueKeyField.getName());

                String prevShard = uniqueDoc.put(id, srsp.getShard());
                if (prevShard != null) {
                    // duplicate detected
                    numFound--;

                    // For now, just always use the first encountered since we
                    // can't currently
                    // remove the previous one added to the priority queue. If
                    // we switched
                    // to the Java5 PriorityQueue, this would be easier.
                    continue;
                    // make which duplicate is used deterministic based on shard
                    // if (prevShard.compareTo(srsp.shard) >= 0) {
                    // TODO: remove previous from priority queue
                    // continue;
                    // }
                }

                EqShardDoc shardDoc = new EqShardDoc();
                shardDoc.id = id;
                shardDoc.shard = srsp.getShard();
                shardDoc.orderInShard = i;
                Object scoreObj = doc.getFieldValue("score");
                if (scoreObj != null) {
                    if (scoreObj instanceof String) {
                        shardDoc.score = Float.parseFloat((String) scoreObj);
                    } else {
                        shardDoc.score = (Float) scoreObj;
                    }
                }

                Object subqIndexObj = doc.getFieldValue("sqidx");
                if (subqIndexObj != null) {
                    if (subqIndexObj instanceof String) {
                        shardDoc.subqIndex = Integer.parseInt((String) subqIndexObj);
                    } else {
                        shardDoc.subqIndex = (Integer) subqIndexObj;
                    }
                }

                Object subqRootsObj = doc.getFieldValue("subqRoots");
                if (subqRootsObj != null) {
                    shardDoc.subqRoots = String.valueOf(subqRootsObj);
                }

                if (rb.getQuery() instanceof EqDisjunctionMaxQuery) {
                    SortField[] shardDocFields = sortFieldsMap.get(shardDoc.subqIndex);
                    NamedList unmarshalledSortFieldValuesforShard = new NamedList();

                    for (int n = 0; n < shardDocFields.length; n++) {
                        unmarshalledSortFieldValuesforShard.add(shardDocFields[n].getField(),
                                unmarshalledSortFieldValues.get(shardDocFields[n].getField()));
                    }

                    shardDoc.sortFieldValues = unmarshalledSortFieldValuesforShard;
                } else {
                    shardDoc.sortFieldValues = unmarshalledSortFieldValues;
                }

                queue.insertWithOverflow(shardDoc);
            } // end for-each-doc-in-response
        } // end for-each-response

        // The queue now has 0 -> queuesize docs, where queuesize <= start +
        // rows
        // So we want to pop the last documents off the queue to get
        // the docs offset -> queuesize
        int resultSize = queue.size() - ss.getOffset();
        resultSize = Math.max(0, resultSize); // there may not be any docs in
                                              // range

        Map<Object, ShardDoc> resultIds = new HashMap<>();
        for (int i = resultSize - 1; i >= 0; i--) {
            ShardDoc shardDoc = queue.pop();
            shardDoc.positionInResponse = i;
            // Need the toString() for correlation with other lists that must
            // be strings (like keys in highlighting, explain, etc)
            resultIds.put(shardDoc.id.toString(), shardDoc);
        }

        // Add hits for distributed requests
        // https://issues.apache.org/jira/browse/SOLR-3518
        rb.rsp.addToLog("hits", numFound);

        SolrDocumentList responseDocs = new SolrDocumentList();
        if (maxScore != null)
            responseDocs.setMaxScore(maxScore);
        responseDocs.setNumFound(numFound);
        responseDocs.setStart(ss.getOffset());
        // size appropriately
        for (int i = 0; i < resultSize; i++)
            responseDocs.add(null);

        // save these results in a private area so we can access them
        // again when retrieving stored fields.
        // TODO: use ResponseBuilder (w/ comments) or the request context?
        rb.resultIds = resultIds;
        rb.setResponseDocs(responseDocs);

        populateNextCursorMarkFromMergedShards(rb);

        if (partialResults) {
            if (rb.rsp.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY) == null) {
                rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY, Boolean.TRUE);
            }
        }
        if (segmentTerminatedEarly != null) {
            final Object existingSegmentTerminatedEarly = rb.rsp.getResponseHeader()
                    .get(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
            if (existingSegmentTerminatedEarly == null) {
                rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, segmentTerminatedEarly);
            } else if (!Boolean.TRUE.equals(existingSegmentTerminatedEarly) && Boolean.TRUE.equals(segmentTerminatedEarly)) {
                rb.rsp.getResponseHeader().remove(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
                rb.rsp.getResponseHeader().add(SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY, segmentTerminatedEarly);
            }
        }
    }

    @Override
    protected void regularFinishStage(ResponseBuilder rb) {
        // TODO Auto-generated method stub
        super.regularFinishStage(rb);

        // limits are exists
        if (rb.getResponseDocs() != null) {

            // limits for sub queries (distributed search)
            Map<Integer, Integer> limitsMap = new TreeMap<>();
            if (rb.getQuery() instanceof EqDisjunctionMaxQuery) {
                for (int i = 0; i < ((EqDisjunctionMaxQuery) rb.getQuery()).getDisjuncts().size(); i++) {
                    int subqIndex = i + 1;
                    if (rb.req.getParams().get("lim_" + i) != null) {
                        int lim = Integer.valueOf(rb.req.getParams().get("lim_" + i).trim());
                        limitsMap.put(subqIndex, lim);
                    } else {
                        limitsMap.put(subqIndex, 0);
                    }
                }
            }

            // apply limits to distributed search
            SolrDocumentList limitedList = new SolrDocumentList();
            Map<Integer, Integer> counterLimits = new HashMap<>();
            for (SolrDocument document : rb.getResponseDocs()) {
                if (document != null && document.get("sqidx") != null) {
                    int subqIndex = Integer.valueOf(document.get("sqidx").toString());
                    if (counterLimits.get(subqIndex) == null) {
                        counterLimits.put(subqIndex, 0);
                    } else {
                        counterLimits.put(subqIndex, counterLimits.get(subqIndex).intValue() + 1);
                    }

                    if ((limitsMap.get(subqIndex).intValue() > counterLimits.get(subqIndex).intValue() || limitsMap.get(subqIndex).intValue() == 0)
                            && limitedList.size() < rb.getSortSpec().getCount()) {
                        limitedList.add(document);
                    }
                }
            }

            // updating response list with limited list
            long numFound = rb.getResponseDocs().getNumFound();
            if (!limitedList.isEmpty()) {
                rb.setResponseDocs(limitedList);
            }
            long limitCount = limitsMap.values().stream().reduce(0, Integer::sum);
            if (limitsMap.values().contains(0) || numFound < limitCount) {
                rb.getResponseDocs().setNumFound(numFound);
            } else {
                rb.getResponseDocs().setNumFound(limitCount);
            }

            // for (SolrDocument document : rb.getResponseDocs()) {
            // document.remove("subqRoots");
            // }
        }
        rb.rsp.getValues().remove("response");
        rb.rsp.addResponse(rb.getResponseDocs());
    }

    @Override
    protected void returnFields(ResponseBuilder rb, ShardRequest sreq) {
        if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
            String keyFieldName = rb.req.getSchema().getUniqueKeyField().getName();
            for (ShardResponse srsp : sreq.responses) {
                SolrDocumentList docs = (SolrDocumentList) srsp.getSolrResponse().getResponse().get("response");
                for (SolrDocument doc : docs) {
                    Object id = doc.getFieldValue(keyFieldName);
                    EqShardDoc sdoc = (EqShardDoc) rb.resultIds.get(id.toString());
                    if (sdoc != null) {
                        if (sdoc.subqIndex != 0) {
                            // if (sdoc.sqidx != 0 && returnSubqIndex) {
                            doc.setField("sqidx", sdoc.subqIndex);
                        } else {
                            doc.remove("sqidx");
                        }
                        doc.setField("subqRoots", sdoc.subqRoots);
                        rb.getResponseDocs().set(sdoc.positionInResponse, doc);
                    }
                }
            }
        }
        super.returnFields(rb, sreq);
    }

}
