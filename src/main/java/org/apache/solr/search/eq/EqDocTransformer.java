package org.apache.solr.search.eq;

import org.apache.lucene.search.EqFieldDoc;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.response.transform.ScoreAugmenter;
import org.apache.solr.search.SolrReturnFields;

public class EqDocTransformer extends ScoreAugmenter {

    final String subqIndex;

    final String subqRoots;

    public EqDocTransformer(String subqIndex, String subqRoots) {
        super(SolrReturnFields.SCORE);
        this.subqIndex = subqIndex;
        this.subqRoots = subqRoots;
    }

    @Override
    public String getName() {
        return "subq";
    }

    @Override
    public void transform(SolrDocument doc, int docid, float score) {
        if (context != null && context instanceof EqBasicResultContext) {
            EqBasicResultContext resultContext = (EqBasicResultContext) context;
            EqFieldDoc fieldDoc = resultContext.getFieldDocsMap().get(docid);
            doc.setField(subqIndex, fieldDoc.subqIndex);
            //doc.setField(subqRoots, fieldDoc.subqRoots); TODO
        }
        super.transform(doc, docid, score);
    }

    @Override
    public void transform(SolrDocument doc, int docid) {
        if (context != null && context instanceof EqBasicResultContext) {
            EqBasicResultContext resultContext = (EqBasicResultContext) context;
            EqFieldDoc fieldDoc = resultContext.getFieldDocsMap().get(docid);
            doc.setField(subqIndex, fieldDoc.subqIndex);
            //doc.setField(subqRoots, fieldDoc.subqRoots); TODO
        }
        super.transform(doc, docid);
    }

}
