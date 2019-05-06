package org.apache.lucene.search;

import java.io.IOException;

public class EqDisjunctionMaxScorer extends EqDisjunctionScorer {

    private String insight = "untouched";
    public int sqmask =0;
    public int sqidx =0;
    public int savedScores=0;

    /*
     * scores keeps the scores of the document from each subquery
     * but the first
     */
    public float[] scores;

    public EqDisjunctionMaxScorer(Weight weight, Scorer[] subScorers, ScoreMode scoreMode) {
        super(weight, subScorers, scoreMode);
        scores = new float[subScorers.length+1];
    }

    @Override
    protected float score(DisiWrapper topList) throws IOException {
        // optimize for the most common case (or so it should be)
        // where topList size is one
        sqidx = ((EqDisiWrapper)topList).scorerIndex;
        sqmask = 0;
        curScore = topList.scorer.score();
        if (topList.next != null) {
            savedScores = 0;
            for (DisiWrapper w = topList; w != null; w = w.next) {
                final int sidx = ((EqDisiWrapper) w).scorerIndex;

                // set sqidx for the _lowest_ (ie most relevant) subquery
                if (sqidx > sidx) sqidx = sidx;

                scores[sidx] = w.scorer.score();
                savedScores++;
                sqmask |= (1 << sidx);
            }

            sqmask &= (~(1<< sqidx));
        }
        return curScore;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }
}
