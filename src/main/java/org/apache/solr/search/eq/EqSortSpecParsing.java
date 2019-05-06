package org.apache.solr.search.eq;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SortSpec;
import org.apache.solr.search.SortSpecParsing;

public class EqSortSpecParsing {

    public static SortSpec parseSortSpec(String sortSpec, SolrQueryRequest req) {
        return parseSortSpecImpl(sortSpec, req.getSchema(), req);
    }

    private static SortSpec parseSortSpecImpl(String sortSpec, IndexSchema schema, SolrQueryRequest optionalReq) {
        // trying to find sub query sorts
        if (optionalReq != null) {
            for (int i = 0; i < optionalReq.getParams().toNamedList().size(); i++) {
                String subSortParam = optionalReq.getParams().get("sort_" + i);
                if (subSortParam != null) {
                    if (sortSpec == null) {
                        sortSpec = subSortParam.trim();
                    } else {
                        sortSpec = sortSpec + ", " + subSortParam.trim();
                    }
                }
            }
        }
        return SortSpecParsing.parseSortSpec(sortSpec, schema);
    }

}
