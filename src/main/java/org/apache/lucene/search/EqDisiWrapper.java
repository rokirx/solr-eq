package org.apache.lucene.search;

import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.Scorer;

public class EqDisiWrapper extends DisiWrapper {

    public int scorerIndex;

    public EqDisiWrapper(Scorer scorer, int scorerIndex ) {
        super(scorer);
        this.scorerIndex = scorerIndex;
    }
}
