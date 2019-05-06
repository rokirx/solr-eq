package org.apache.solr.handler.component.eq;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.PriorityQueue;
import org.apache.solr.common.SolrException;

// used by distributed search to merge results.
public class EqShardFieldSortedHitQueue extends PriorityQueue<EqShardDoc> {

    /** Stores a comparator corresponding to each field being sorted by */
    protected Comparator<EqShardDoc>[] comparators;

    /** Stores the sort criteria being used. */
    protected SortField[] fields;

    /** The order of these fieldNames should correspond to the order of sort field values retrieved from the shard */
    protected List<String> fieldNames = new ArrayList<>();

    protected Map<Integer, Comparator<EqShardDoc>[]> comparatorsMap;
    protected Map<Integer, SortField[]> fieldsMap;
    protected Map<Integer, List<String>> fieldNamesMap = new TreeMap<>();

    public EqShardFieldSortedHitQueue(SortField[] fields, int size, IndexSearcher searcher) {
        super(size);
        final int n = fields.length;
        // noinspection unchecked
        comparators = new Comparator[n];
        this.fields = new SortField[n];
        for (int i = 0; i < n; ++i) {

            // keep track of the named fields
            SortField.Type type = fields[i].getType();
            if (type != SortField.Type.SCORE && type != SortField.Type.DOC) {
                fieldNames.add(fields[i].getField());
            }

            String fieldname = fields[i].getField();
            comparators[i] = getCachedComparator(fields[i], searcher);

            if (fields[i].getType() == SortField.Type.STRING) {
                this.fields[i] = new SortField(fieldname, SortField.Type.STRING, fields[i].getReverse());
            } else {
                this.fields[i] = new SortField(fieldname, fields[i].getType(), fields[i].getReverse());
            }
        }
    }

    public EqShardFieldSortedHitQueue(Map<Integer, SortField[]> fieldsMap, int size, IndexSearcher searcher) {
        super(size);
        // noinspection unchecked
        comparatorsMap = new TreeMap<>();
        this.fieldsMap = new TreeMap<>();
        for (Map.Entry<Integer, SortField[]> fieldEntry : fieldsMap.entrySet()) {
            final int n = fieldEntry.getValue().length;
            Integer subqIndex = fieldEntry.getKey();
            SortField[] fields = fieldEntry.getValue();

            for (int i = 0; i < n; i++) {
                // keep track of the named fields
                SortField.Type type = fields[i].getType();
                if (type != SortField.Type.SCORE && type != SortField.Type.DOC) {
                    if (!fieldNamesMap.containsKey(subqIndex)) {
                        fieldNamesMap.put(subqIndex, new ArrayList<>());
                    }
                    fieldNamesMap.get(subqIndex).add(fields[i].getField());
                }

                if (!comparatorsMap.containsKey(subqIndex)) {
                    comparatorsMap.put(subqIndex, new Comparator[n]);
                }
                String fieldname = fields[i].getField();
                comparatorsMap.get(subqIndex)[i] = getCachedComparator(fields[i], searcher, subqIndex);

                if (!this.fieldsMap.containsKey(subqIndex)) {
                    this.fieldsMap.put(subqIndex, new SortField[n]);
                }
                if (fields[i].getType() == SortField.Type.STRING) {
                    this.fieldsMap.get(subqIndex)[i] = new SortField(fieldname, SortField.Type.STRING, fields[i].getReverse());
                } else {
                    this.fieldsMap.get(subqIndex)[i] = new SortField(fieldname, fields[i].getType(), fields[i].getReverse());
                }
            }
        }
    }

    @Override
    protected boolean lessThan(EqShardDoc docA, EqShardDoc docB) {
        if (docA.subqIndex != docB.subqIndex) {
            return docB.subqIndex < docA.subqIndex;
        }

        // If these docs are from the same shard, then the relative order
        // is how they appeared in the response from that shard.
        if (docA.shard == docB.shard) {
            // if docA has a smaller position, it should be "larger" so it
            // comes before docB.
            // This will handle sorting by docid within the same shard

            // comment this out to test comparators.
            return !(docA.orderInShard < docB.orderInShard);
        }

        // run comparators
        final int currentDocsSubqIndex = docA.subqIndex;
        if (currentDocsSubqIndex != 0) {
            return lessThanProcess(this.fieldsMap.get(currentDocsSubqIndex), this.comparatorsMap.get(currentDocsSubqIndex), docA, docB);
        }
        return lessThanProcess(fields, comparators, docA, docB);
    }

    private boolean lessThanProcess(SortField[] fields, Comparator[] comparators, EqShardDoc docA, EqShardDoc docB) {
        final int n = comparators.length;
        int c = 0;
        for (int i = 0; i < n && c == 0; i++) {
            c = (fields[i].getReverse()) ? comparators[i].compare(docB, docA) : comparators[i].compare(docA, docB);
        }

        // solve tiebreaks by comparing shards (similar to using docid)
        // smaller docid's beat larger ids, so reverse the natural ordering
        if (c == 0) {
            c = -docA.shard.compareTo(docB.shard);
        }

        return c < 0;
    }

    Comparator<EqShardDoc> getCachedComparator(SortField sortField, IndexSearcher searcher, int subqIndex) {
        SortField.Type type = sortField.getType();
        if (type == SortField.Type.SCORE) {
            return (o1, o2) -> {
                final float f1 = o1.score;
                final float f2 = o2.score;
                if (o1.subqIndex == o2.subqIndex) {
                    if (f1 < f2)
                        return -1;
                    if (f1 > f2) {
                        return 1;
                    }
                    return 0;
                } else {
                    return o1.subqIndex > o2.subqIndex ? -1 : 1;
                }
            };
        } else if (type == SortField.Type.REWRITEABLE) {
            try {
                sortField = sortField.rewrite(searcher);
            } catch (IOException e) {
                throw new SolrException(SERVER_ERROR, "Exception rewriting sort field " + sortField, e);
            }
        }
        if (subqIndex != 0) {
            return comparatorFieldComparator(sortField, subqIndex);
        }
        return comparatorFieldComparator(sortField);
    }

    Comparator<EqShardDoc> getCachedComparator(SortField sortField, IndexSearcher searcher) {
        return getCachedComparator(sortField, searcher, 0);
    }

    abstract class ShardComparator implements Comparator<EqShardDoc> {
        final SortField sortField;
        final String fieldName;
        final int fieldNum;
        final Map<Integer, Integer> fieldNumsMap = new TreeMap<>();

        public ShardComparator(SortField sortField) {
            this.sortField = sortField;
            this.fieldName = sortField.getField();
            int fieldNum = 0;
            for (int i = 0; i < fieldNames.size(); i++) {
                if (fieldNames.get(i).equals(fieldName)) {
                    fieldNum = i;
                    break;
                }
            }
            this.fieldNum = fieldNum;
        }

        public ShardComparator(SortField sortField, int subqIndex) {
            this.sortField = sortField;
            this.fieldName = sortField.getField();
            int fieldNum = 0;
            List<String> subqFildNames = fieldNamesMap.get(subqIndex);
            for (int i = 0; i < subqFildNames.size(); i++) {
                if (subqFildNames.get(i).equals(fieldName)) {
                    fieldNum = i;
                    break;
                }
            }
            this.fieldNumsMap.put(subqIndex, fieldNum);
            this.fieldNum = fieldNum;
        }

        Object sortVal(EqShardDoc shardDoc) {
            if (shardDoc.subqIndex != 0) {
                assert (shardDoc.sortFieldValues.getName(fieldNumsMap.get(shardDoc.subqIndex)).equals(fieldName));
                List lst = (List) shardDoc.sortFieldValues.getVal(fieldNumsMap.get(shardDoc.subqIndex));
                return lst.get(shardDoc.orderInShard);
            }
            assert (shardDoc.sortFieldValues.getName(fieldNum).equals(fieldName));
            List lst = (List) shardDoc.sortFieldValues.getVal(fieldNum);
            return lst.get(shardDoc.orderInShard);
        }
    }

    Comparator<EqShardDoc> comparatorFieldComparator(SortField sortField) {
        final FieldComparator fieldComparator = sortField.getComparator(0, 0);
        return new ShardComparator(sortField) {
            // Since the PriorityQueue keeps the biggest elements by default,
            // we need to reverse the field compare ordering so that the
            // smallest elements are kept instead of the largest... hence
            // the negative sign.
            @Override
            public int compare(final EqShardDoc o1, final EqShardDoc o2) {
                // noinspection unchecked
                return -fieldComparator.compareValues(sortVal(o1), sortVal(o2));
            }
        };
    }

    Comparator<EqShardDoc> comparatorFieldComparator(SortField sortField, int subqIndex) {
        final FieldComparator fieldComparator = sortField.getComparator(0, 0);
        return new ShardComparator(sortField, subqIndex) {
            // Since the PriorityQueue keeps the biggest elements by default,
            // we need to reverse the field compare ordering so that the
            // smallest elements are kept instead of the largest... hence
            // the negative sign.
            @Override
            public int compare(final EqShardDoc o1, final EqShardDoc o2) {
                // noinspection unchecked
                return -fieldComparator.compareValues(sortVal(o1), sortVal(o2));
            }
        };
    }
}
