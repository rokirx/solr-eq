package org.apache.lucene.search;

public class EqFieldDoc extends FieldDoc {

    /**
     * If using the '&lt;&lt;' operator:
     * the position of the subquery this hit is coming from.
     *
     * Otherwise the default value is 0.
     *
     * sqidx is initialized in the EqTopFieldCollector.populateResults
     */
    public int subqIndex;

    /**sub queries in which there is a document */
    public float[] subqScores;

    public EqFieldDoc(int doc, float score, Object[] fields, int subqIndex, float[] subqScores) {
        super(doc, score, fields);
        this.subqIndex = subqIndex;
        this.subqScores = subqScores;
    }
}
