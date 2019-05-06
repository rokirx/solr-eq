/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search;


import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.PriorityQueue;

/**
 * Expert: A hit queue for sorting by hits by terms in more than one field.
 *
 * Notice by R.K.
 *
 * We need to copypaste this class, because the extension is prevented by private constructor.
 * The private constructor creates the comparators, there is no possibility to control which
 * comparators are created. In the case of RelevanceComparator we want to override the method
 * setScorer to prevent the wrapping of our EqDisjunctionMaxScorer into ScoreCachingWrappingScorer.
 * Again, even if we would write our own wrapping scorer, the following code would in any case
 *
 *
 * if (!(scorer instanceof ScoreCachingWrappingScorer)) {
 *     this.scorer = new ScoreCachingWrappingScorer(scorer);
 * } else {
 *     this.scorer = scorer;
 * }
 *
 * A possible solution to this issue could be to introduce a marker interface CachingScorer
 * which would indicate whether the wrapping is needed. Then the following change in
 * RelevancComaparater would do:
 *
 * if (!(scorer instanceof CachingScorer)) {
 *     this.scorer = new ScoreCachingWrappingScorer(scorer);
 * } else {
 *     this.scorer = scorer;
 * }
 *
 *
 * @lucene.experimental
 * @since 2.9
 * @see IndexSearcher#search(Query,int,Sort)
 */
public abstract class EqFieldValueHitQueue<T extends EqEntry> extends PriorityQueue<T> {

    public int sqidx;

    /**
     * An implementation of {@link FieldValueHitQueue} which is optimized in case
     * there is just one comparator.
     */
    private static final class OneComparatorFieldValueHitQueue<T extends EqEntry> extends EqFieldValueHitQueue<T> {

        private final int oneReverseMul;
        private final FieldComparator<?> oneComparator;
        private final EqCopyValueIf oC;
        public int docBase;

        public OneComparatorFieldValueHitQueue(EqSortField[] fields, int size, int sqidx) {
            super(fields, size);
            assert fields.length == 1;
            oneComparator = comparators[0];
            oC = (EqCopyValueIf) oneComparator;
            oneReverseMul = reverseMul[0];
            this.sqidx = sqidx;
        }

        /**
         * Returns whether <code>hitA</code> is less relevant than <code>hitB</code>.
         * @param hitA Entry
         * @param hitB Entry
         * @return <code>true</code> if document <code>hitA</code> should be sorted after document <code>hitB</code>.
         */
        @Override
        protected boolean lessThan(final EqEntry hitA, final EqEntry hitB) {

            assert hitA != hitB;
            assert hitA.slot != hitB.slot;

            int c = 0;

            // TODO: what if both a and b are not from current segment?
            if(oC.getDocBase() != hitA.docBase){
                if(oC.getDocBase() != hitB.docBase){
                    c = oneReverseMul * oC.compare(hitA, hitB);
                }else{
                    c = oneReverseMul * oC.compare(hitA, hitB.slot);
                }
            }else if (oC.getDocBase() != hitB.docBase){
                c = - oneReverseMul * oC.compare(hitB, hitA.slot);
            }else{
                c = oneReverseMul * oneComparator.compare(hitA.slot, hitB.slot);
            }

            if (c != 0) {
                return c > 0;
            }

            // avoid random sort order that could lead to duplicates (bug #31241):
            return hitA.doc > hitB.doc;
        }
    }

    /**
     * An implementation of {@link FieldValueHitQueue} which is optimized in case
     * there is more than one comparator.
     */
    private static final class MultiComparatorsFieldValueHitQueue<T extends EqEntry> extends EqFieldValueHitQueue<T> {

        public MultiComparatorsFieldValueHitQueue(EqSortField[] fields, int size) {
            super(fields, size);
        }

        @Override
        protected boolean lessThan(final EqEntry hitA, final EqEntry hitB) {

            assert hitA != hitB;
            assert hitA.slot != hitB.slot;

            int numComparators = comparators.length;
            for (int i = 0; i < numComparators; ++i) {
                final int c = reverseMul[i] * comparators[i].compare(hitA.slot, hitB.slot);
                if (c != 0) {
                    // Short circuit
                    return c > 0;
                }
            }

            // avoid random sort order that could lead to duplicates (bug #31241):
            return hitA.doc > hitB.doc;
        }

    }

    // prevent instantiation and extension.
    public EqFieldValueHitQueue(EqSortField[] fields, int size) {
        super(size);

        // When we get here, fields.length is guaranteed to be > 0, therefore no
        // need to check it again.

        // All these are required by this class's API - need to return arrays.
        // Therefore even in the case of a single comparator, create an array
        // anyway.
        this.fields = fields;
        int numComparators = fields.length;
        comparators = new FieldComparator<?>[numComparators];
        reverseMul = new int[numComparators];
        for (int i = 0; i < numComparators; ++i) {
            EqSortField field = fields[i];

            reverseMul[i] = field.getReverse() ? -1 : 1;
            // TODO: implement correct sorting if other sort fields are used together with score
            if (field.getType() == SortField.Type.SCORE){
                if (fields.length>1)
                    throw new UnsupportedOperationException("cannot combine sort by score with other fields");
                comparators[i] = new EqRelevanceComparator(size, this.sqidx);
            }else {
                comparators[i] = field.getEqComparator(size, i, this.sqidx);
            }
        }
    }

    /**
     * Creates a hit queue sorted by the given list of fields.
     *
     * <p><b>NOTE</b>: The instances returned by this method
     * pre-allocate a full array of length <code>numHits</code>.
     *
     * @param fields
     *          SortField array we are sorting by in priority order (highest
     *          priority first); cannot be <code>null</code> or empty
     * @param size
     *          The number of hits to retain. Must be greater than zero.
     */
    public static EqFieldValueHitQueue<EqEntry> create(EqSortField[] fields, int size, int sqidx) {

        if (fields.length == 0) {
            throw new IllegalArgumentException("Sort must contain at least one field");
        }

        if (fields.length == 1) {
            return new OneComparatorFieldValueHitQueue<EqEntry>(fields, size, sqidx);
        } else {
            return new MultiComparatorsFieldValueHitQueue<EqEntry>(fields, size);
        }
    }

    public FieldComparator<?>[] getComparators() {
        return comparators;
    }

    public int[] getReverseMul() {
        return reverseMul;
    }

    public LeafFieldComparator[] getComparators(LeafReaderContext context) throws IOException {
        LeafFieldComparator[] comparators = new LeafFieldComparator[this.comparators.length];
        for (int i = 0; i < comparators.length; ++i) {
            comparators[i] = this.comparators[i].getLeafComparator(context);
        }
        return comparators;
    }

    /** Stores the sort criteria being used. */
    protected final EqSortField[] fields;
    protected final FieldComparator<?>[] comparators;
    protected final int[] reverseMul;

    @Override
    protected abstract boolean lessThan (final EqEntry a, final EqEntry b);

    /**
     * Given a queue Entry, creates a corresponding FieldDoc
     * that contains the values used to sort the given document.
     * These values are not the raw values out of the index, but the internal
     * representation of them. This is so the given search hit can be collated by
     * a MultiSearcher with other search hits.
     *
     * @param entry The Entry used to create a FieldDoc
     * @return The newly created FieldDoc
     * @see IndexSearcher#search(Query,int,Sort)
     */
    EqFieldDoc fillFields(final EqEntry entry) {
        final int n = comparators.length;
        final Object[] fields = new Object[n];
        for (int i = 0; i < n; ++i) {
            fields[i] = comparators[i].value(entry.slot);
        }
        //if (maxscore > 1.0f) doc.score /= maxscore;   // normalize scores
        return new EqFieldDoc(entry.doc, entry.score, fields, entry.sqmask, entry.scores);
    }

    /** Returns the SortFields being used by this hit queue. */
    SortField[] getFields() {
        return fields;
    }
}
