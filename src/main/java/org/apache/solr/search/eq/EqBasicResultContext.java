package org.apache.solr.search.eq;

import java.util.Map;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.EqFieldDoc;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.BasicResultContext;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;

public class EqBasicResultContext extends BasicResultContext {

    private Map<Integer, EqFieldDoc> fieldDocsMap;

    public EqBasicResultContext(DocList docList, ReturnFields returnFields, SolrIndexSearcher searcher, Query query, SolrQueryRequest req,
            Map<Integer, EqFieldDoc> fieldDocsMap) {
        super(docList, returnFields, searcher, query, req);
        this.fieldDocsMap = fieldDocsMap;
    }

    public Map<Integer, EqFieldDoc> getFieldDocsMap() {
        return fieldDocsMap;
    }

    public void setFieldDocsMap(Map<Integer, EqFieldDoc> fieldDocsMap) {
        this.fieldDocsMap = fieldDocsMap;
    }

}
