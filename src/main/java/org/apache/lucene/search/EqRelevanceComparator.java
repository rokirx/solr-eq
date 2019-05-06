package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

public class EqRelevanceComparator extends FieldComparator<Float> implements LeafFieldComparator, EqCopyValueIf {
    private final float[] scores;
    private float bottom;
    private Scorable scorer;
    private float topValue;
    public int sqidx;
    int docBase;

    /** Creates a new comparator based on relevance for {@code numHits}. */
    public EqRelevanceComparator(int numHits, int sqidx) {
        scores = new float[numHits];
        this.sqidx = sqidx;
    }

    @Override
    public int compare(int slot1, int slot2) {
        return Float.compare(scores[slot2], scores[slot1]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        float score = scorer.score();
        assert !Float.isNaN(score);
        return Float.compare(score, bottom);
    }

    public int compareBottom(EqEntry e) {
        return Float.compare(e.compValues[sqidx].scoreValue, bottom);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        scores[slot] = scorer.score();
        assert !Float.isNaN(scores[slot]);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
        docBase = context.docBase;
        return this;
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = scores[bottom];
    }

    @Override
    public void setTopValue(Float value) {
        topValue = value;
    }

    @Override
    public void setScorer(Scorable scorer) {
        // wrap with a ScoreCachingWrappingScorer so that successive calls to
        // score() will not incur score computation over and
        // over again.
        if (!(scorer instanceof EqDisjunctionMaxScorer))
            throw new UnsupportedOperationException("No other scorers than EqDisjunctionMaxScorer supported here");

        this.scorer = scorer;
    }

    @Override
    public Float value(int slot) {
        return Float.valueOf(scores[slot]);
    }

    // Override because we sort reverse of natural Float order:
    @Override
    public int compareValues(Float first, Float second) {
        // Reversed intentionally because relevance by default
        // sorts descending:
        return second.compareTo(first);
    }

    @Override
    public int compareTop(int doc) throws IOException {
        float docValue = scorer.score();
        assert !Float.isNaN(docValue);
        return Float.compare(docValue, topValue);
    }

    public void copyValue(EqComparatorValue v, int doc) throws IOException{
        v.scoreValue = scorer.score();
    }

    public int getDocBase(){
        return docBase;
    }

    public int compare(EqEntry a, EqEntry b){
        return Float.compare(a.compValues[sqidx].scoreValue, b.compValues[sqidx].scoreValue);
    }

    public int compare(EqEntry a, int slot){
        return Float.compare(a.compValues[sqidx].scoreValue, scores[slot]);
    }
}