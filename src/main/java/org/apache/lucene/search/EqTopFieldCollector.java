package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.LeafReaderContext;

import javax.naming.ldap.UnsolicitedNotification;

public abstract class EqTopFieldCollector extends TopDocsCollector<EqEntry> {

    /*
     * Stores the maximum score value encountered, needed for normalizing. If
     * document scores are not tracked, this value is initialized to NaN.
     */
    float[] maxScores;

    EqFieldValueHitQueue<EqEntry>[] pqs;
    LeafFieldComparator[] comparators;
    int[] reverseMul;
    int[] bucketSize;
    int[] sqHits;
    Sort[] sorts;
    int[] limits;
    EqEntry[] bottoms;
    EqEntry docEntry = new EqEntry();
    boolean[] queueFull;

    /* support for fast match: terminate collect as early as possible */
    int lowestCollectingQueue;
    int collectedHits;
    int trackMaxScoresMask;

    /* per doc value */
    private float[] subqScores;


    private static final ScoreDoc[] EMPTY_SCOREDOCS = new ScoreDoc[0];
    final int numHits;
    int docBase;
    boolean earlyTerminated = false;

    private boolean topDocsAlreadyCalledOnce = false;
    
    private TopDocs savedTopDocs = null;

    private static byte[] sqidxBit = {
            // 10 -> 1, 100 -> 2 and so on
            0, 0, 1, 0, 2, 0, 0, 0,   3, 0, 0, 0, 0, 0, 0, 0,  //  0..15
            4, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,  // 16..31
            5, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,  // 32..47
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,  // 48..63
            6, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,  // 64..
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,

            7, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,  // 128..
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
    };


    public EqTopFieldCollector(EqFieldValueHitQueue<EqEntry>[] pqs, int numHits) {
        super(null);
        this.pqs         = pqs;
        this.numHits     = numHits;
        this.bottoms     = new EqEntry[pqs.length];
        this.maxScores   = new float[pqs.length];
        this.queueFull   = new boolean[pqs.length];
        this.comparators = new LeafFieldComparator[pqs.length];
        this.reverseMul  = new int[pqs.length];
        this.bucketSize  = new int[pqs.length];
        this.sqHits = new int[pqs.length];
        this.lowestCollectingQueue = pqs.length;
        this.collectedHits = 0;
    }

    @Override
    public int getTotalHits() {
        return collectedHits;
    }

    @Override
    public TopDocs topDocs(int start, int howMany) {

        if (topDocsAlreadyCalledOnce)
            throw new UnsupportedOperationException("cannot call topDocs more than once");
        topDocsAlreadyCalledOnce = true;

        // In case pq was populated with sentinel values, there might be less
        // results than pq.size(). Therefore return all results until either
        // pq.size() or collectedHits.
        int size = getTopDocsSize();

        if( start < 0 || howMany <= 0 ){
            throw new IllegalArgumentException("invalid values for start or howMany");
        }

        if (start >= size || start >= numHits) {
            savedTopDocs = newTopDocs(null, start);
            return savedTopDocs;
        }

        // fix howMany.
        howMany = Math.min(Math.min(size,numHits) - start, howMany);
        EqFieldDoc[] results = new EqFieldDoc[howMany];

        // pq's pop() returns the 'least' element in the queue, therefore need
        // to discard the first ones, until we reach the requested range.
        // Note that this loop will usually not be executed, since the common
        // usage should be that the caller asks for the last howMany results. However
        // it's needed here for completeness.
        int i = size - start - howMany;
        for(int j = pqs.length; j>0; j--){
            if (i==0) break;
            EqFieldValueHitQueue<EqEntry> q = pqs[j-1];
            while(q.size()>0){
                q.pop();
                i--;
                if (i==0) break;
            }
        }

        // Get the requested results from pq.
        populateResults(results, howMany);
        
        savedTopDocs = newTopDocs(results, start);
        return savedTopDocs;
    }
    
    public TopDocs getSavedTopDocs() {
        return savedTopDocs;
    }

    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
        if (results == null) {
            results = EMPTY_SCOREDOCS;
            // Set maxScore to NaN, in case this is a maxScore tracking
            // collector.
            //maxScore = Float.NaN;
        }

        // If this is a maxScoring tracking collector and there were no results,
        // TODO Fix maxScore
        return new TopFieldDocs(new TotalHits(collectedHits, totalHitsRelation), results, ((EqFieldValueHitQueue<EqEntry>) pqs[0]).getFields());
    }

    @Override
    protected void populateResults(ScoreDoc[] results, int howMany) {
        int i = howMany-1;
        while(i>=0){
            for (int j = pqs.length-1; j>=0; j--){
                EqFieldValueHitQueue<EqEntry> q = pqs[j];
                while(q.size() > 0){
                    EqFieldDoc doc = q.fillFields(q.pop());
                    doc.subqIndex = j+1;
                    results[i] = doc;
                    if (i==0) break;
                    i--;
                }
            }
            if(i==0) break;
        }
    }

    @Override
    protected int topDocsSize() {
        // In case pq was populated with sentinel values, there might be less
        // results than pq.size(). Therefore return all results until either
        // pq.size() or collectedHits.
        int size = getTopDocsSize();
        return size > numHits ? numHits : size;
    }

    public int getTopDocsSize(){
        int size = 0;
        for (EqFieldValueHitQueue<EqEntry> q: pqs){
            size += q.size();
        }
        return size;
    }

    final void updateBottom(int doc, float score, int subqIndex) {
        bottoms[subqIndex-1].doc = docBase + doc;
        bottoms[subqIndex-1].score = score;
        bottoms[subqIndex-1] = (EqEntry) pqs[subqIndex-1].updateTop();
        bottoms[subqIndex-1].scores = subqScores;
    }

    /*
    final void add(int slot, int doc, float score, EqDisjunctionMaxScorer scorer) {
        float scores[] = new float[pqs.length];
        int idx = scorer.sqidx-1;

        if (scorer.sqmask>0){
            for(int i = 0; i < scorer.savedScores; i++){
                scores[i] = scorer.scores[i];
            }
        }

        bottoms[idx] = pqs[idx].add(new EqEntry(slot, docBase, docBase + doc, score, scorer.sqmask, scores));
        queueFull[idx] = sqHits[idx] == bucketSize[idx];
    }
     */

    final EqEntry top(int subQindex) {
        return pqs[subQindex - 1].top();
    }

    private static class SimpleStackedFieldCollector extends EqTopFieldCollector {

        final boolean trackDocScores;
        final boolean trackMaxScore;
        final boolean trackTotalHits;
        
        @Override
        public ScoreMode scoreMode() {
            for (int i = 0; i < sorts.length; i++) {
                if (sorts[i].needsScores())
                    return ScoreMode.COMPLETE;
            }
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        public SimpleStackedFieldCollector( Sort[] sorts,
                                            int[] limits,
                                            EqFieldValueHitQueue<EqEntry>[] queues,
                                            int numHits,
                                            boolean fillFields,
                                            boolean trackDocScores,
                                            boolean trackMaxScore,
                                            boolean trackTotalHits ) {
            super(queues, numHits);

            //, needsScores(sorts) || trackDocScores || trackMaxScore
            this.trackDocScores = trackDocScores;
            this.trackMaxScore = trackMaxScore;

            this.sorts = sorts;
            this.limits = limits;

            if (trackMaxScore) {
                // TODO R.K fix
                maxScores[0] = Float.NEGATIVE_INFINITY; // otherwise we would keep NaN
            }

            this.trackTotalHits = trackTotalHits;
        }
        
        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

            docBase = context.docBase;

            // set up the initial state for all the collector lists

            for (int i = 0; i < this.sorts.length; i++) {

//                final Sort indexSort = context.reader().getMetaData().getSort();
//                final boolean canEarlyStopComparing = indexSort != null &&
//                        canEarlyTerminate(sort, indexSort);
//                final boolean canEarlyTerminate = trackTotalHits == false &&
//                        trackMaxScore == false &&
//                        canEarlyStopComparing;
//                final int initialTotalHits = collectedHits;


                // TODO optimize for with/without score / with/without field sorts
//                comparators.set(i,((FieldValueHitQueue<EqEntry>)pqs[i]).getComparators(context));
//                reverseMul.set(i,((FieldValueHitQueue<EqEntry>)pqs[i]).getReverseMul());

                // TODO (R.K.) check if we can use this meaningfully
                // final Sort indexSort =
                // context.reader().getMetaData().getSort();
                // final boolean canEarlyStopComparing = indexSort != null &&
                // false;

                // TODO (R.K.): we replaced the last canEarlyTerminate call with false.
                // Still have to evaluate whether this can be useful in our case
                // (of stacked queries)
                // canEarlyTerminate(sort, indexSort);
                //final boolean canEarlyTerminate = trackTotalHits == false && trackMaxScore == false
                //        && false /* canEarlyStopComparing */;
                final boolean canEarlyTerminate = false;
                final int initialTotalHits = collectedHits;

                bucketSize[i] = limits[i] > 0 ? Math.min(limits[i],numHits) : numHits;

                EqFieldValueHitQueue q = pqs[i];
                LeafFieldComparator[] c = q.getComparators(context);

                if (c.length == 1){
                    reverseMul[i] = q.getReverseMul()[0];
                    comparators[i] = c[0];
                }else{
                    // TODO: Unit tests for multi comparators
                    reverseMul[i] = 1;
                    comparators[i] = new MultiLeafFieldComparator(c, q.getReverseMul());
                }
            }

            // TODO real value for mayNeedScoresTwice
            return new EqMultiComparatorLeafCollector( comparators,reverseMul, true /* mayNeedScoresTwice */) {
                
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    super.setScorer(scorer);
                }
                
                @Override
                public void collect(int doc) throws IOException {

                    // local state of the collect cycle is described through variables:
                    //
                    // score   - the current score of a doc
                    // scores  - array with score for each subquery
                    // sqmask  - a bitmask of the subqueries the doc is coming from
                    docEntry.score  = scorer.score();
                    docEntry.scores = scorer.scores;
                    docEntry.sqmask = scorer.sqmask;

                    // scorer.sqidx starts with 1
                    int sqidx = scorer.sqidx - 1;

                    // TODO: at some point optimize for trackMaxScore
                    // For the time being just track.
                    // To decide: should we update maxScores also in those
                    // buckets where the document could only potentially be
                    // collected, or only update them when the document actually
                    // gets collected.
                    // Currently we implement the simplest approach: update only
                    // those buckets which at least try to collect the document
                    // (but maybe still reject it)

                    collectedHits++;

                    if (sqidx > lowestCollectingQueue){
                        // TODO: update max scores where needed.
                        return;
                    }

                    // true if docBase has the correct value for the doc
                    // it may happen that we re-collect a doc from some earlier
                    // segment so the docBase value may be outdated, then
                    // docBaseMatch should be false and docBaseBottom keeps the
                    // correct docBase for the doc
                    boolean docBaseMatch = true;
                    int docBaseBottom = 0;

                    while(true) {
                        if (queueFull[sqidx]) {
                            // Find the queue which is ready to accept the doc.
                            // As long as sqmask has at least one bit set we have
                            // queues which might accept the document

                            // sqidx is correctly initialized for the first loop.
                            // If the queue doesn't accept doc the sqidx is adjusted
                            // to point at the next valid queue

                            // TODO check cond collectedAllCompetitiveHits

                            // testCompareBottomWithSavedDoc
                            final int cmp = reverseMul[sqidx] * (
                                    docBaseMatch ?
                                            comparators[sqidx].compareBottom(doc) :
                                            ((EqCopyValueIf)comparators[sqidx]).compareBottom(docEntry));

                            // doc does not enter this queue
                            if (cmp <= 0) {
                                // cmp <= 0 means the doc cannot be collected into the current
                                // queue. we have to check whether it can be collected into
                                // the lower queues.
                                // TODO conditions canEarlyStopComparing, canEarlyTerminate

                                // no more queues waiting for this doc
                                if (0 == docEntry.sqmask) {
                                    // The doc has been rejected by the queue and no more queues
                                    // will accept the doc. If the rejecting queue is limited we
                                    // have to consider this document as 'no hit' and decrease total hits
                                    if (limits[sqidx] > 0) {
                                        collectedHits--;
                                    }
                                    return;
                                }

                                // cmp > 0, docEntry.sqmask > 0:
                                // there are more queues waiting, still try to collect the doc
                                if (limits[sqidx] == 0) {
                                    // rejecting queue is not limited, just move to the next doc,
                                    // no need to move this doc into another bucket
                                    return;
                                }

                                // cmp > 0, docEntry.sqmask > 0, limits[sqidx] > 0
                                // initialize the next value of sqidx and continue.
                                // we have to identify which next queue should collect the doc
                                int mask = docEntry.sqmask;
                                int lowestOne = mask & -mask;

                                // delete lowest one bit from sqmask
                                docEntry.sqmask &= (~lowestOne);

                                // what is the index of the next one bit?
                                if (0 != (mask & 0xff)) {
                                    sqidx = sqidxBit[lowestOne] - 1;
                                } else if (0 != ((mask >>> 8) & 0xff)) {
                                    sqidx = 7 + sqidxBit[lowestOne >>> 8];
                                } else if (0 != ((mask >>> 16) & 0xff)) {
                                    sqidx = 15 + sqidxBit[lowestOne >>> 16];
                                } else if (0 != ((mask >>> 24) & 0xff)) {
                                    sqidx = 23 + sqidxBit[lowestOne >>> 24];
                                }
                                // TODO : fix score here

                                // we have initialized the new value of sqidx, adjusted the sqmask
                                // now proceed with the loop trying to collect the doc
                                continue;
                            }

                            // cmp > 0:
                            // collect the doc into current queue, ie the sort value of the doc
                            // is high enough to replace some collected doc with lower sort value

                            if(limits[sqidx] == 0) {
                                // insert doc into unlimited queue
                                EqEntry btm = bottoms[sqidx];
                                comparators[sqidx].copy(btm.slot, doc);

                                // inlined call to updateBottom
                                // just overwrite values of bottom because no queue is accepting bottom
                                btm.doc = docBaseMatch ? docBase + doc : docBaseBottom + doc;
                                btm.score = docEntry.score;
                                btm.sqmask = scorer.sqmask;
                                btm.docBase = docBaseMatch ? docBase : docBaseBottom;
                                bottoms[sqidx] = pqs[sqidx].updateTop();
                                comparators[sqidx].setBottom(bottoms[sqidx].slot);
                                break;
                            }

                            // cmp > 0, limits[sqidx] > 0
                            // insert doc into limited queue
                            EqEntry btm = bottoms[sqidx];

                            // what happens:
                            // btm is removed from the current limited queue, but it might be
                            // collected by another queue depending on btm.sqmask > 0.
                            // so we have to save the values of btm for going into the next loop.
                            // on the other hand we have to update btm values to be able to call
                            // updateTop on the current queue which will return the next bottom
                            // element to save in the bottoms. In short we have to:
                            //
                            // A. if btm is not needed just update bottom -> break
                            // B. if it is needed save it first then update bottom -> continue

                            if(docBaseMatch) {
                                // copy the doc values into the previous bottom slot
                                comparators[sqidx].copy(btm.slot, doc);
                            }else{
                                throw new UnsupportedOperationException("copy");
                            }

                            // TODO: this may be a good place to copy the comparator values of doc for future.
                            // this doc still may be collected by another queue/comparator.

                            // inlined call to updateBottom
                            if (btm.sqmask == 0) {
                                // btm will now contain the values of the current doc being collected
                                // the corresponding comparator comparator[sqidx] has already copied the
                                // sort values of the doc. Because of btm.sqmask == 0 we don't care about
                                // overriding the values of bottom, because it will not be collected anymore
                                collectedHits--;
                                // just overwrite values of bottom because no queue is accepting bottom
                                btm.doc = docBaseMatch ? docBase + doc : docBaseBottom + doc;
                                btm.docBase = docBaseMatch ? docBase : docBaseBottom;
                                btm.score = docEntry.score;
                                btm.sqmask = docEntry.sqmask;

                                // TODO: keep only comparators which might be useful
                                // sqmask > 0 means the doc might be recollected in future, so
                                // save the sort values in the corresponding EqEntry
                                if (docEntry.sqmask > 0) {
                                    for (int i = 0; i < comparators.length; i++) {
                                        if (true)
                                            throw new UnsupportedOperationException("XXX");
                                        ((EqCopyValueIf) comparators[i]).copyValue(btm.compValues[i], doc);
                                    }
                                }

                                bottoms[sqidx] = pqs[sqidx].updateTop();
                                comparators[sqidx].setBottom(bottoms[sqidx].slot);
                                // TODO: btm.scores
                                // bottoms[sqidx-1].scores = scores;
                                break;
                            }

                            // btm.sqmask > 0
                            // a doc was excluded from the limited queue but still is a candidate
                            // for a subsequent queue

                            // set up everything to collect the btm doc

                            // first adjust the value of squidx to point at the next queue
                            // collecting the doc
                            int mask = btm.sqmask;
                            int bit = mask ^ (mask & (mask - 1));
                            int newSqidx = 0;
                            int newSqbits = btm.sqmask & (~bit);
                            if (0 != (mask & 0xff)) {
                                newSqidx = sqidxBit[bit] - 1;
                            } else if (0 != ((mask >>> 8) & 0xff)) {
                                newSqidx = 7 + sqidxBit[bit >>> 8];
                            } else if (0 != ((mask >>> 16) & 0xff)) {
                                newSqidx = 15 + sqidxBit[bit >>> 16];
                            } else if (0 != ((mask >>> 24) & 0xff)) {
                                newSqidx = 23 + sqidxBit[bit >>> 24];
                            }

                            // btm doc still on the current segment
                            if (btm.docBase == docBase) {

                                // swap the loop state with bottom:

                                // doc:
                                int newDoc = btm.doc - docBase;
                                btm.doc = docBase + doc;
                                btm.docBase = docBase;
                                doc = newDoc;

                                // TODO: score:
                                btm.score = docEntry.score;
                                // score = ???

                                // TODO: scores:
                                // scores = ???

                                // sqmask
                                btm.sqmask = docEntry.sqmask;
                                docEntry.sqmask = newSqbits;

                                bottoms[sqidx] = pqs[sqidx].updateTop();
                                comparators[sqidx].setBottom(bottoms[sqidx].slot);

                                // sqidx:
                                sqidx = newSqidx;
                                // TODO: check sqidx for correction
                            } else {
                                // btm.doc is not on the current segment anymore
                                docBaseMatch = false;
                                int newDoc = btm.doc - btm.docBase;
                                docBaseBottom = btm.docBase;
                                btm.doc = docBase + doc;
                                btm.docBase = docBase;
                                doc = newDoc;
                                // TODO: score
                                btm.sqmask = docEntry.sqmask;
                                docEntry.sqmask = newSqbits;
                                docEntry.compValues = btm.compValues;
                                bottoms[sqidx] = pqs[sqidx].updateTop();
                                comparators[sqidx].setBottom(bottoms[sqidx].slot);
                                sqidx = newSqidx;
                            }

                            continue;
                        }

                        // queue is not full, just insert the doc
                        final int slot = sqHits[sqidx];
                        sqHits[sqidx]++;

                        // Copy doc sort value into the comparator
                        if (docBaseMatch) {
                            comparators[sqidx].copy(slot, doc);
                        } else {
                            new UnsupportedOperationException("copy int comparator");
                        }

                        // inline add method

                        float lscores[] = new float[pqs.length];
                        if (scorer.sqmask > 0) {
                            for (int i = 0; i < scorer.savedScores; i++) {
                                lscores[i] = scorer.scores[i];
                            }
                        }

                        EqEntry e = docBaseMatch ?
                                new EqEntry(slot, docBase, docBase + doc, docEntry.score, docEntry.sqmask, lscores) :
                                new EqEntry(slot, docBaseBottom, docBaseBottom + doc, docEntry.score, docEntry.sqmask, lscores);

                        // if the doc is collected into limited queue AND the doc might be collected
                        // by another queue, then copy the sort values for future use
                        if (docEntry.sqmask > 0 && limits[sqidx] > 0) {

                            // TODO: take only those comparators which might be used in future
                            for (int i = 0; i < comparators.length; i++) {
                                ((EqCopyValueIf) comparators[i]).copyValue(e.compValues[i], doc);
                            }
                        }

                        bottoms[sqidx] = pqs[sqidx].add(e);

                        if (queueFull[sqidx] = sqHits[sqidx] == bucketSize[sqidx]) {
                            comparators[sqidx].setBottom(bottoms[sqidx].slot);
                        }

                        return;
                    }
                }
            };
        }

    }

    static boolean canEarlyTerminate(Sort searchSort, Sort indexSort) {
        final SortField[] fields1 = searchSort.getSort();
        final SortField[] fields2 = indexSort.getSort();
        // early termination is possible if fields1 is a prefix of fields2
        if (fields1.length > fields2.length) {
            return false;
        }
        return Arrays.asList(fields1).equals(Arrays.asList(fields2).subList(0, fields1.length));
    }

    static int estimateRemainingHits(int hitCount, int doc, int maxDoc) {
        double hitRatio = (double) hitCount / (doc + 1);
        int remainingDocs = maxDoc - doc - 1;
        int remainingHits = (int) (remainingDocs * hitRatio);
        return remainingHits;
    }

    private static abstract class EqMultiComparatorLeafCollector implements LeafCollector {

        final boolean mayNeedScoresTwice;
        final LeafFieldComparator[] comparators;
        final int[] reverseMul;
        EqDisjunctionMaxScorer scorer;

        EqMultiComparatorLeafCollector(LeafFieldComparator[] comparators, int[] reverseMul, boolean mayNeedScoresTwice) {
            this.mayNeedScoresTwice = mayNeedScoresTwice;
            this.comparators = comparators;
            this.reverseMul = reverseMul;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {

            if (!(scorer instanceof EqDisjunctionMaxScorer)){
                throw new IllegalArgumentException("need to implement caching scorer");
            }

            for(LeafFieldComparator c: comparators) {
                c.setScorer(scorer);
            }

            this.scorer = (EqDisjunctionMaxScorer) scorer;
        }
    }

    public static EqTopFieldCollector create(Sort[] sorts){
        return create(sorts, 10);
    }

    public static EqTopFieldCollector create(Sort[] sorts, int numHits){
        return create(sorts, new int[sorts.length], numHits);
    }

    public static EqTopFieldCollector create(
            Sort[] sorts,
            int[] limits,
            int numHits){
        return create(sorts, limits, numHits, null,
                false, false, false, true);
    }

    private static EqSortField[] toEqSortFields(SortField[] fields){
        EqSortField[] eqFields = new EqSortField[fields.length];
        for(int i = 0; i<eqFields.length; i++){
            eqFields[i] = new EqSortField(fields[i]);
        }
        return eqFields;
    }

    public static EqTopFieldCollector create(
            Sort[] sorts,
            int[] limits,
            int numHits,
            FieldDoc after,
            boolean fillFields,
            boolean trackDocScores,
            boolean trackMaxScore,
            boolean trackTotalHits) {

        if (sorts.length == 0) {
            throw new IllegalArgumentException("Sort must contain at least one field");
        }

        if (numHits <= 0) {
            throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        EqFieldValueHitQueue<EqEntry>[] queues = new EqFieldValueHitQueue[sorts.length];

        for (int i = 0; i < sorts.length; i++) {
            int queueSize = limits[i] != 0 ? limits[i] : numHits;
            //TODO: track instantiation, why is scorer not initialized?
            EqSortField[] fields = toEqSortFields(sorts[i].getSort());
            queues[i] = EqFieldValueHitQueue.create(fields, queueSize, i);
        }

        if (after == null) {
            return new SimpleStackedFieldCollector(
                    sorts,
                    limits,
                    queues,
                    numHits,
                    fillFields,
                    trackDocScores,
                    trackMaxScore,
                    trackTotalHits);
        } else {
            throw new UnsupportedOperationException("we don't support multiple collectors with after parameter");
        }
    }
}
