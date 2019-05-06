package org.apache.solr.search.eq;

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.search.SolrReturnFields;

public class EqSolrReturnFields extends SolrReturnFields {

    public static final String SUBQ_INDEX = "sqidx";
    public static final String SUBQ_ROOTS = "subqRoots";

    public EqSolrReturnFields(SolrQueryRequest req) {
        super(req);
    }

    @Override
    public DocTransformer getTransformer() {
        return new EqDocTransformer(SUBQ_INDEX, SUBQ_ROOTS);
    }

    @Override
    public boolean wantsField(String name) {
        boolean wantsField = super.wantsField(name);
        if (name.equals(SUBQ_INDEX) || name.equals(SUBQ_ROOTS)) {
            return true;
        }
        return wantsField;
    }

}
