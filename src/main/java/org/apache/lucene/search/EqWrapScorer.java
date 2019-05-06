package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;

public class EqWrapScorer extends Scorer {

    public Scorer in;

    public EqWrapScorer(Scorer in) {
        super(in.getWeight());
        this.in = in;
    }

    @Override
    public int docID() {
        return this.in.docID();
    }

    @Override
    public float score() throws IOException {
        return this.in.score();
    }

    @Override
    public DocIdSetIterator iterator() {
        return this.in.iterator();
    }

    @Override
    public final TwoPhaseIterator twoPhaseIterator() {
        return in.twoPhaseIterator();
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

}
