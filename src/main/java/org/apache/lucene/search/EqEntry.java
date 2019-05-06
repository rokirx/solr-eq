package org.apache.lucene.search;

import org.apache.lucene.search.FieldValueHitQueue.Entry;

/**
 * Extension of ScoreDoc to also store the
 * {@link FieldComparator} slot.
 */
public class EqEntry extends Entry {

    public int sqmask;
    public float[] scores;

    // try to keep the sort values for documents which are collected into
    // limited queue: they may have to be collected to another queue
    public EqComparatorValue[] compValues;

    /*
     * We have to track the segment of the docs.
     *
     * If a doc is expelled from a limiting queue it may be collected
     * by some other queue using new comparators which may be using
     * other segment than the doc has come from. This docBase will help
     * to identify those cases.
     */
    public int docBase;

    public EqEntry(){
        super(0,0);
    }

    public EqEntry(int slot, int docBase, int doc, float score, int sqmask, float[] scores) {
        super(slot, doc);
        this.sqmask = sqmask;
        this.scores = scores;
        this.docBase = docBase;
        this.compValues = new EqComparatorValue[scores.length];
        for(int i=0; i<compValues.length; i++){
            compValues[i] = new EqComparatorValue();
        }
    }
}
