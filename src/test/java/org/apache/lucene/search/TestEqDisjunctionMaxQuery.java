package org.apache.lucene.search;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.junit.Ignore;
import org.junit.Test;

@TestRuleLimitSysouts.Limit(bytes = 200000)
public class TestEqDisjunctionMaxQuery extends LuceneTestCase {

    public static final float SCORE_COMP_THRESH = 0.0000f;

    /**
     * Similarity to eliminate tf, idf and lengthNorm effects to isolate test
     * case.
     * 
     * <p>
     * same as TestRankingSimilarity in TestRanking.zip from
     * http://issues.apache.org/jira/browse/LUCENE-323
     * </p>
     */
    private static class TestSimilarity extends ClassicSimilarity {

        public TestSimilarity() {
        }

        @Override
        public float tf(float freq) {
            if (freq > 0.0f)
                return 1.0f;
            else
                return 0.0f;
        }

        @Override
        public float lengthNorm(int length) {
            // Disable length norm
            return 1;
        }

        @Override
        public float idf(long docFreq, long docCount) {
            return 1.0f;
        }
    }

    private static class InvariantTotalHits {
        int val;
        public InvariantTotalHits(int val) {
            this.val = val;
        }
    }

    public static InvariantTotalHits invHits(int val) {
        return new InvariantTotalHits(val);
    }

    private static class InvariantTopDocsSize {
        int val;

        public InvariantTopDocsSize(int val) {
            this.val = val;
        }
    }

    public static InvariantTopDocsSize invTDSize(int val) {
        return new InvariantTopDocsSize(val);
    }

    private static class InvariantSqidx {
        int[] val;

        public InvariantSqidx(int[] val) {
            this.val = val;
        }
    }

    public static InvariantSqidx invSqidx(int... ints){ return new InvariantSqidx(ints); }


    private class ColFac {

        public IndexReader reader = null;

        public Sort[] sort;
        public int[] limit;
        public int numHits = 10;

        public InvariantTotalHits hits = null;
        public InvariantTopDocsSize tdSize = null;
        public InvariantSqidx sqidx = null;


        public ColFac withNumHits(int hits){
            numHits = hits;
            return this;
        }

        public void setInvariants(InvariantTopDocsSize tdSize, InvariantTotalHits hits){
            this.tdSize = tdSize;
            this.hits = hits;
        }

        public void setInvariants(InvariantTopDocsSize tdSize, InvariantTotalHits hits, InvariantSqidx sqidx) {
            this.tdSize = tdSize;
            this.hits = hits;
            this.sqidx = sqidx;
        }

        public ColFac(Sort[] sort) {
            this.sort = sort;
            this.limit = new int[sort.length];
        }

        public ColFac(Sort[] sort, int[] limit) {
            this.sort = sort;
            this.limit = limit;
        }

        public ColFac(IndexReader r, Sort[] sort, int[] limit) {
            this.reader = r;
            this.sort = sort;
            this.limit = limit;
        }

        public EqTopFieldCollector create() {
            return EqTopFieldCollector.create(sort, limit, numHits);
        }

        public EqTopFieldCollector create(int nHits) {
            return EqTopFieldCollector.create(sort, limit, nHits);
        }

        public void check(Query q, int[] docs) throws Exception {
            check(q, docs, null);
        }

        public void check(Query q, int tdSize, int hits, int[] docs, int[] sqidx) throws Exception {
            check(q, invTDSize(tdSize), invHits(hits), docs, sqidx);
        }

        public void check(Query q, int tdSize, int hits, int[] docs) throws Exception {
            check(q, invTDSize(tdSize), invHits(hits), docs, null);
        }

        public void check(Query q, int[] docs, int[] sqidx) throws Exception {
            check(q, tdSize, hits, docs, sqidx);
        }

        public void check(Query q, TestData d) throws Exception{
            check(q, d.tdSize, d.tHits, d.dIds);
        }

        private IndexSearcher searcher(){
            if (null == reader) return indexSearcher;
            IndexSearcher s = new IndexSearcher(reader);
            s.setSimilarity(similarity);
            return s;
        }

        public void check(Query q, InvariantTopDocsSize tdSize, InvariantTotalHits hits, int[] docs, int[] sqidx) throws Exception {

            EqTopFieldCollector c = create();
            IndexSearcher s = searcher();
            s.search(q, c);
            if (null != tdSize) {
                assertEquals("getTopDocsSize", tdSize.val, c.getTopDocsSize());
            }
            if (null != hits) {
                assertEquals("totalHits", hits.val, c.getTotalHits());
            }

            TopDocs topDocs = c.topDocs(0, 10);

            assertEquals("scoreDocs.length", docs.length, topDocs.scoreDocs.length);

            for (int i = 0; i < docs.length; i++) {
                assertEquals("docId", docs[i], topDocs.scoreDocs[i].doc);
                if (null != sqidx) {
                    assertEquals("subquery index", sqidx[i], ((EqFieldDoc) topDocs.scoreDocs[i]).subqIndex);
                } else if (null != this.sqidx) {
                    assertEquals("subquery index", this.sqidx.val[i], ((EqFieldDoc) topDocs.scoreDocs[i]).subqIndex);
                }
            }
            assertEquals("too many docs", docs.length, topDocs.scoreDocs.length);
        }
    }

    public Similarity similarity = new TestSimilarity();
    public Directory index;
    public IndexReader indexReader;
    public IndexSearcher indexSearcher;

    /* the same with 2 segments */
    public Directory indexSeg;
    public IndexReader indexReaderSeg;
    public IndexSearcher indexSearcherSeg;

    public Sort byScore = new Sort(new SortField("score", SortField.Type.SCORE));
    public Sort byScoreDesc = new Sort(new SortField("score", SortField.Type.SCORE, true));
    public Sort byId = new Sort(new SortField("id_i", SortField.Type.INT));
    public Sort byIdDesc = new Sort(new SortField("id_i", SortField.Type.INT, true));
    public Sort byLong = new Sort(new SortField("id_l", SortField.Type.LONG));
    public Sort byLongDesc = new Sort(new SortField("id_l", SortField.Type.LONG, true));
    public Sort byTxt = new Sort(new SortField("title_s", SortField.Type.STRING));
    public Sort byTxtDesc = new Sort(new SortField("title_s", SortField.Type.STRING, true));
    public Sort byTxtVal = new Sort(new SortField("title_s", SortField.Type.STRING_VAL));
    public Sort byTxtValDesc = new Sort(new SortField("title_s", SortField.Type.STRING_VAL, true));
    public Sort byFloat = new Sort(new SortField( "id_fl", SortField.Type.FLOAT));
    public Sort byFloatDesc = new Sort(new SortField( "id_fl", SortField.Type.FLOAT));
    public Sort byDouble = new Sort(new SortField( "id_dbl", SortField.Type.DOUBLE));
    public Sort byDoubleDesc = new Sort(new SortField( "id_dbl", SortField.Type.DOUBLE, true));

    private static final FieldType nonAnalyzedType = new FieldType(TextField.TYPE_STORED);
    static {
        nonAnalyzedType.setTokenized(false);
    }

    private static final Document doc(String id, String sid) {
        Document d = new Document();
        d.add(newField("id", id, nonAnalyzedType));
        d.add(new NumericDocValuesField("id_i", Integer.parseInt(id)));
        d.add(newTextField("sid_s", sid, Field.Store.YES));
        d.add(new SortedDocValuesField("title_s", new BytesRef(("title " + id).getBytes())));
        d.add(newTextField("text", "text " + id, Field.Store.YES));
        d.add(new NumericDocValuesField("id_l", Integer.parseInt(id)));
        d.add(new FloatDocValuesField( "id_fl", Float.parseFloat(id) ));
        d.add(new DoubleDocValuesField("id_dbl", Double.parseDouble(id)));
        return d;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        index = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), index,
                newIndexWriterConfig(new MockAnalyzer(random())).setSimilarity(similarity).setMergePolicy(newLogMergePolicy()));

        writer.addDocument(doc("0", "a"));
        writer.addDocument(doc("1", "a b"));
        writer.addDocument(doc("2", "a b c"));
        writer.addDocument(doc("3", "a b c d"));

        writer.forceMerge(1);
        indexReader = getOnlyLeafReader(writer.getReader());
        writer.close();
        indexSearcher = new IndexSearcher(indexReader);
        indexSearcher.setSimilarity(similarity);




        indexSeg = newDirectory();
        writer = new RandomIndexWriter(random(), indexSeg,
                newIndexWriterConfig(new MockAnalyzer(random())).setSimilarity(similarity).setMergePolicy(newLogMergePolicy()));

        writer.addDocument(doc("0", "a"));
        writer.addDocument(doc("1", "a b"));
        writer.commit();
        writer.addDocument(doc("2", "a b c"));
        writer.addDocument(doc("3", "a b c d"));

        writer.forceMerge(4);
        indexReaderSeg = writer.getReader();
        writer.close();
        indexSearcherSeg = new IndexSearcher(indexReaderSeg);
        indexSearcherSeg.setSimilarity(similarity);
    }

    @Override
    public void tearDown() throws Exception {
        indexReader.close();
        index.close();

        indexReaderSeg.close();
        indexSeg.close();
        super.tearDown();
    }

    private static class TestData {
        public int tdSize;
        public int tHits;
        public int[] dIds;

        public TestData(int tdSize, int tHits, int[] dIds){
            this.tdSize=tdSize;
            this.tHits=tHits;
            this.dIds=dIds;
        }
    }

    private static int[] i(int... ints){ return ints;}
    private static int[] limit(int... ints){ return ints;}
    private static int[] docs(int... ints){ return ints;}
    private static int[] sqidx(int... ints){ return ints;}
    private static Sort[] sort(Sort...sorts){ return sorts;}

    private static Query id(int id){ return tq("id", "" + id); }
    private static Query sid(String s){ return tq("sid_s", s); }
    private static EqDisjunctionMaxQuery q(Query a){ return new EqDisjunctionMaxQuery(Arrays.asList(a)); }
    private static EqDisjunctionMaxQuery q(Query a, Query b){ return new EqDisjunctionMaxQuery(Arrays.asList(a,b));}

    private void assertSearch(EqDisjunctionMaxQuery q, EqTopFieldCollector c, int tdSize, int tHits, int[] dIds) throws IOException {
        assertSearch(q, c, tdSize, tHits, dIds, null);
    }

    private void assertSearch(EqDisjunctionMaxQuery q, EqTopFieldCollector c, int tdSize, int tHits, int[] dIds, int[] subqIdxs) throws IOException {
        indexSearcher.search(q, c);
        assertEquals("getTopDocsSize", tdSize, c.getTopDocsSize());
        assertEquals("totalHits", tHits, c.getTotalHits());
        TopDocs topDocs = c.topDocs(0, 10);

        assertEquals("scoreDocs.length", dIds.length, topDocs.scoreDocs.length);

        for (int i = 0; i < dIds.length; i++) {
            assertEquals("docId", dIds[i], topDocs.scoreDocs[i].doc);
            if (null != subqIdxs) {
                assertEquals("subquery index", subqIdxs[i], ((EqFieldDoc) topDocs.scoreDocs[i]).subqIndex);
            }
        }
    }

    public void testEqTopFieldDocCollectorBasics() throws Exception {
        ColFac col = new ColFac(sort(byScore, byId), limit(1, 2));
        col.setInvariants(invTDSize(2), invHits(2), invSqidx(1, 2));

        col.check(q(id(0), id(2)), docs(0, 2));
        col.check(q(id(2), id(0)), docs(2, 0));
    }

    public void testNonExistingfield() throws Exception {
        ColFac col = new ColFac(sort(byScore, byId), limit(1, 2));
        col.setInvariants(invTDSize(1), invHits(1));

        col.check(q(id(0), tq("idx:2")), docs(0), sqidx(1));
        col.check(q(tq("idx:2"), id(0)), docs(0), sqidx(2));
        col.check(q(tq("idx:2"), tq("idy:0")), 0, 0, docs(), sqidx());
    }

    public void testFirstPosition() throws Exception {
        int[][] expOrder = { { 0, 1, 2, 3 }, { 1, 0, 2, 3 }, { 2, 0, 1, 3 }, { 3, 0, 1, 2 }, };

        ColFac col = new ColFac(sort(byScore, byId));
        col.setInvariants(invTDSize(4), invHits(4), invSqidx(1, 2, 2, 2));

        for (int i = 0; i < 4; i++) {
            col.check(q(id(i), sid("a")), expOrder[i]);
        }

        col.check(q(id(2), id(0)), 2, 2, docs(2, 0), sqidx(1, 2));
    }

    public void testFullQueue() throws Exception {

        ColFac col = new ColFac(sort(byScore, byId));

        EqDisjunctionMaxQuery q = q(sid("a"),id(0));
        col.withNumHits(1).check(q, 1, 4, docs(0));
        col.withNumHits(2).check(q, 2, 4, docs(0,1));
        col.withNumHits(3).check(q, 3, 4, docs(0,1,2));
        col.withNumHits(4).check(q, 4, 4, docs(0,1,2,3));
    }

    public void testFillingSecondQueue() throws Exception {

        ColFac col = new ColFac(new Sort[] { byScore, byId });

        TestData[] docId0 = {
                new TestData(2, 4, docs(0)),
                new TestData(3, 4, docs(0,1)),
                new TestData(4, 4, docs(0,1,2)),
                new TestData(4, 4, docs(0,1,2,3)),
        };

        TestData[] docId1 = {
                new TestData(2, 4, docs(1)),
                new TestData(3, 4, docs(1,0)),
                new TestData(4, 4, docs(1,0,2)),
                new TestData(4, 4, docs(1,0,2,3)),
        };

        TestData[] docId2 = {
                new TestData(2, 4, docs(2)),
                new TestData(3, 4, docs(2,0)),
                new TestData(4, 4, docs(2,0,1)),
                new TestData(4, 4, docs(2,0,1,3)),
        };

        TestData[] docId3 = {
                new TestData(2, 4, docs(3)),
                new TestData(3, 4, docs(3,0)),
                new TestData(4, 4, docs(3,0,1)),
                new TestData(4, 4, docs(3,0,1,2)),
        };

        for(int i=0; i<4; i++) {
            col.withNumHits(i+1).check(q(id(0),sid("a")), docId0[i]);
            col.withNumHits(i+1).check(q(id(1),sid("a")), docId1[i]);
            col.withNumHits(i+1).check(q(id(2),sid("a")), docId2[i]);
            col.withNumHits(i+1).check(q(id(3),sid("a")), docId3[i]);
        }
    }

    public void testOneLimitedQ() throws Exception {

        // tests with limit = 1
        ColFac col = new ColFac(sort(byId), limit(1));
        col.setInvariants(invTDSize(1), invHits(1), invSqidx(1));

        col.check(q(sid("a")), docs(0));
        col.check(q(sid("b")), docs(1));
        col.check(q(sid("c")), docs(2));
        col.check(q(sid("d")), docs(3));

        col = new ColFac(sort(byIdDesc), limit(1));
        col.setInvariants(invTDSize(1), invHits(1), invSqidx(1));

        col.check(q(sid("a")), docs(3));
        col.check(q(sid("b")), docs(3));
        col.check(q(sid("c")), docs(3));
        col.check(q(sid("d")), docs(3));

        // tests with limit = 2
        col = new ColFac(sort(byId), limit(2));
        col.setInvariants(invTDSize(2), invHits(2), invSqidx(1, 1));

        col.check(q(sid("a")), docs(0, 1));
        col.check(q(sid("b")), docs(1, 2));
        col.check(q(sid("c")), docs(2, 3));
        col.check(q(sid("d")), invTDSize(1), invHits(1), docs(3), sqidx(1));

        col = new ColFac(sort(byIdDesc), limit(2));
        col.setInvariants(invTDSize(2), invHits(2), invSqidx(1, 1));

        col.check(q(sid("a")), docs(3, 2));
        col.check(q(sid("b")), docs(3, 2));
        col.check(q(sid("c")), docs(3, 2));
        col.check(q(sid("d")), invTDSize(1), invHits(1), docs(3), sqidx(1));

        // tests with limit = 3
        col = new ColFac(sort(byId), limit(3));
        col.setInvariants(invTDSize(3), invHits(3), invSqidx(1, 1, 1));

        col.check(q(sid("a")), docs(0, 1, 2));
        col.check(q(sid("b")), docs(1, 2, 3));
        col.check(q(sid("c")), invTDSize(2), invHits(2), docs(2, 3), sqidx(1, 1));
        col.check(q(sid("d")), invTDSize(1), invHits(1), docs(3), sqidx(1));

        col = new ColFac(sort(byIdDesc), limit(3));
        col.setInvariants(invTDSize(3), invHits(3), invSqidx(1, 1, 1));

        col.check(q(sid("a")), docs(3, 2, 1));
        col.check(q(sid("b")), docs(3, 2, 1));
        col.check(q(sid("c")), invTDSize(2), invHits(2), docs(3, 2), sqidx(1, 1));
        col.check(q(sid("d")), invTDSize(1), invHits(1), docs(3), sqidx(1));
    }

    public void testOneLimitedQueueWithSmallBuckets() throws Exception {

        // tests with limit = 1
        ColFac col = new ColFac(sort(byId), limit(1));
        col.numHits = 1;
        col.setInvariants(invTDSize(1), invHits(1), invSqidx(1));

        col.check(q(sid("a")), docs(0));
        col.check(q(sid("b")), docs(1));
        col.check(q(sid("c")), docs(2));
        col.check(q(sid("d")), docs(3));

        col = new ColFac(sort(byIdDesc), limit(1));
        col.numHits = 1;
        col.setInvariants(invTDSize(1), invHits(1), invSqidx(1));

        col.check(q(sid("a")), docs(3));
        col.check(q(sid("b")), docs(3));
        col.check(q(sid("c")), docs(3));
        col.check(q(sid("d")), docs(3));

        // tests with limit = 2
        col = new ColFac(sort(byId), limit(2));
        col.numHits = 1;
        col.setInvariants(invTDSize(1), invHits(1), invSqidx(1));

        col.check(q(sid("a")), docs(0));
        col.check(q(sid("b")), docs(1));
        col.check(q(sid("c")), docs(2));
        col.check(q(sid("d")), docs(3));

        col = new ColFac(sort(byIdDesc), limit(2));
        col.numHits = 1;
        col.setInvariants(invTDSize(1), invHits(1), invSqidx(1));

        col.check(q(sid("a")), docs(3));
        col.check(q(sid("b")), docs(3));
        col.check(q(sid("c")), docs(3));
        col.check(q(sid("d")), docs(3));

        // tests with limit = 3
        col = new ColFac(sort(byId), limit(3));
        col.numHits = 1;
        col.setInvariants(invTDSize(1), invHits(1), invSqidx(1));

        col.check(q(sid("a")), docs(0));
        col.check(q(sid("b")), docs(1));
        col.check(q(sid("c")), docs(2));
        col.check(q(sid("d")), docs(3));

        col = new ColFac(sort(byIdDesc), limit(3));
        col.numHits = 1;
        col.setInvariants(invTDSize(1), invHits(1), invSqidx(1));

        col.check(q(sid("a")), docs(3));
        col.check(q(sid("b")), docs(3));
        col.check(q(sid("c")), docs(3));
        col.check(q(sid("d")), docs(3));
    }

    public void testOneUnlimitedQ() throws Exception {
        ColFac col = new ColFac(sort(byId));
        col.numHits = 1;
        col.sqidx = new InvariantSqidx(sqidx(1));

        col.check(q(sid("a")), 1, 4, docs(0));
        col.check(q(sid("b")), 1, 3, docs(1));
        col.check(q(sid("c")), 1, 2, docs(2));
        col.check(q(sid("d")), 1, 1, docs(3));

        col = new ColFac(sort(byIdDesc));
        col.numHits = 1;
        col.sqidx = new InvariantSqidx(sqidx(1));

        col.check(q(sid("a")), 1, 4, docs(3));
        col.check(q(sid("b")), 1, 3, docs(3));
        col.check(q(sid("c")), 1, 2, docs(3));
        col.check(q(sid("d")), 1, 1, docs(3));

        col = new ColFac(sort(byId));
        col.numHits = 2;
        col.sqidx = new InvariantSqidx(sqidx(1, 1));

        col.check(q(sid("a")), 2, 4, docs(0, 1));
        col.check(q(sid("b")), 2, 3, docs(1, 2));
        col.check(q(sid("c")), 2, 2, docs(2, 3));
        col.check(q(sid("d")), invTDSize(1), invHits(1), docs(3), sqidx(1));

        col = new ColFac(sort(byIdDesc));
        col.numHits = 2;
        col.sqidx = new InvariantSqidx(sqidx(1, 1));

        col.check(q(sid("a")), 2, 4, docs(3, 2));
        col.check(q(sid("b")), 2, 3, docs(3, 2));
        col.check(q(sid("c")), 2, 2, docs(3, 2));
        col.check(q(sid("d")), invTDSize(1), invHits(1), docs(3), sqidx(1));

        col = new ColFac(sort(byId));
        col.numHits = 3;
        col.sqidx = new InvariantSqidx(sqidx(1, 1, 1));

        col.check(q(sid("a")), 3, 4, docs(0, 1, 2));
        col.check(q(sid("b")), 3, 3, docs(1, 2, 3));
        col.check(q(sid("c")), invTDSize(2), invHits(2), docs(2, 3), sqidx(1, 1));
        col.check(q(sid("d")), invTDSize(1), invHits(1), docs(3), sqidx(1));

        col = new ColFac(sort(byIdDesc));
        col.numHits = 3;

        col.check(q(sid("a")), invTDSize(3), invHits(4), docs(3, 2, 1), sqidx(1, 1, 1));
        col.check(q(sid("b")), invTDSize(3), invHits(3), docs(3, 2, 1), sqidx(1, 1, 1));
        col.check(q(sid("c")), invTDSize(2), invHits(2), docs(3, 2), sqidx(1, 1));
        col.check(q(sid("d")), invTDSize(1), invHits(1), docs(3), sqidx(1));
    }

    public void testDocPropagation() throws Exception {
        ColFac col = new ColFac(sort(byId, byId));
        col.numHits = 2;
        col.sqidx = new InvariantSqidx(sqidx(1, 1));

        col.check(q(sid("a"), sid("b")), 2, 4, docs(0, 1));
        col.check(q(sid("b"), sid("a")), 3, 4, docs(1, 2));

        col = new ColFac(sort(byId, byId));
        col.numHits = 1;
        col.sqidx = new InvariantSqidx(sqidx(1));

        col.check(q(sid("a"), sid("b")), 1, 4, docs(0));
        col.check(q(sid("b"), sid("a")), 2, 4, docs(1));

        col = new ColFac(sort(byId, byId));
        col.numHits = 3;
        col.sqidx = new InvariantSqidx(sqidx(1, 1, 1));

        col.check(q(sid("a"), sid("b")), 3, 4, docs(0, 1, 2));
        col.check(q(sid("b"), sid("a")), 4, 4, docs(1, 2, 3));

        col = new ColFac(sort(byId, byId));
        col.numHits = 4;

        col.check(q(sid("a"), sid("b")), 4, 4, docs(0, 1, 2, 3), sqidx(1, 1, 1, 1));
        col.check(q(sid("b"), sid("a")), 4, 4, docs(1, 2, 3, 0), sqidx(1, 1, 1, 2));
    }

    public void testDocPropagationFromLimited() throws Exception {
        ColFac col = new ColFac(sort(byId, byId), limit(1, 0));
        col.numHits = 1;
        col.sqidx = new InvariantSqidx(sqidx(1));

        col.check(q(sid("a"), sid("b")), 2, 4, docs(0));
        col.check(q(sid("b"), sid("a")), 2, 4, docs(1));

        col = new ColFac(sort(byIdDesc, byId), limit(1, 0));
        col.numHits = 1;
        col.sqidx = new InvariantSqidx(sqidx(1));

        col.check(q(sid("a"), sid("b")), 2, 3, docs(3));
        col.check(q(sid("b"), sid("a")), 2, 4, docs(3));
    }

    public void testWithFewSegments() throws Exception {
        Directory index = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), index,
                newIndexWriterConfig(new MockAnalyzer(random())).setSimilarity(similarity).setMergePolicy(newLogMergePolicy()));

        writer.addDocument(doc("0", "a"));
        writer.addDocument(doc("1", "a b"));
        writer.commit();
        writer.addDocument(doc("2", "a b c"));
        writer.addDocument(doc("3", "a b c d"));

        writer.forceMerge(4);
        IndexReader indexReader = writer.getReader();
        writer.close();

        ColFac col = new ColFac(indexReader, sort(byIdDesc, byIdDesc), limit(2,1));
        col.check(q(sid("a"), sid("b")), docs(3, 2, 1));

        col = new ColFac(indexReader, sort(byId, byIdDesc), limit(2,1));
        col.check(q(sid("a"), sid("b")), docs(0, 1, 3));


        col = new ColFac(indexReader, sort(byId, byId), limit(2,1));
        col.check(q(sid("a"), sid("b")), docs(0, 1, 2));

        col = new ColFac(indexReader, sort(byIdDesc, byId), limit(2,1));
        col.check(q(sid("a"), sid("b")), docs(3, 2, 1));

        col = new ColFac(indexReader, sort(byIdDesc, byIdDesc), limit(1,2));
        col.check(q(sid("a"), sid("b")), docs(3, 2, 1));

        col = new ColFac(indexReader, sort(byId, byIdDesc), limit(1,2));
        col.check(q(sid("a"), sid("b")), docs(0, 3, 2));

        col = new ColFac(indexReader, sort(byId, byId), limit(1,2));
        col.check(q(sid("a"), sid("b")), docs(0, 1, 2));

        col = new ColFac(indexReader, sort(byIdDesc, byId), limit(1,2));
        col.check(q(sid("a"), sid("b")), docs(3, 1, 2));

        indexReader.close();
        index.close();
    }

    public void testCompareBottomWithSavedDoc() throws Exception {
        Directory index = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), index,
                newIndexWriterConfig(new MockAnalyzer(random())).setSimilarity(similarity).setMergePolicy(newLogMergePolicy()));

        writer.addDocument(doc("0", "a"));
        writer.addDocument(doc("1", "a b"));
        writer.commit();
        writer.addDocument(doc("5", "a b c"));
        writer.addDocument(doc("3", "a b c d"));

        writer.forceMerge(4);
        IndexReader indexReader = writer.getReader();
        writer.close();
        ColFac col;

        col = new ColFac(indexReader, sort(byId, byIdDesc), limit(1,2));
        col.check(q(sid("a"), sid("b")), docs(0, 2 /* id=5 */, 3 /* id=3 */));
        /* collect stages:
         * doc 0 (0:0) -> [0] []
         * doc 1 (0:1) -> [0] [1]
         * doc 2 (1:0) -> [0] [2 1]
         * doc 3 (1:1) -> [0] [2 3]
         */

        //col = new ColFac(indexReader, sort(byId, byId), limit(1,2));
        //col.check(q(sid("a"), sid("c")), docs(0, 3 /* id=3 */, 2 /* id=5 */));

        indexReader.close();
        index.close();
    }

    public void testCompareBottomWithLong() throws Exception {
        Directory index = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), index,
                newIndexWriterConfig(new MockAnalyzer(random())).setSimilarity(similarity).setMergePolicy(newLogMergePolicy()));

        writer.addDocument(doc("0", "a"));
        writer.addDocument(doc("1", "a b"));
        writer.commit();
        writer.addDocument(doc("5", "a b c"));
        writer.addDocument(doc("3", "a b c d"));

        writer.forceMerge(4);
        IndexReader indexReader = writer.getReader();
        writer.close();
        ColFac col;

        col = new ColFac(indexReader, sort(byLong, byLongDesc), limit(1,2));
        col.check(q(sid("a"), sid("b")), docs(0, 2 /* id=5 */, 3 /* id=3 */));

        col = new ColFac(indexReader, sort(byLong, byLong), limit(1,2));
        col.check(q(sid("a"), sid("c")), docs(0, 3 /* id=3 */, 2 /* id=5 */));

        indexReader.close();
        index.close();
    }

    public void testCompareBottomWithString() throws Exception {
        Directory index = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), index,
                newIndexWriterConfig(new MockAnalyzer(random())).setSimilarity(similarity).setMergePolicy(newLogMergePolicy()));

        writer.addDocument(doc("0", "a"));
        writer.addDocument(doc("1", "a b"));
        writer.commit();
        writer.addDocument(doc("5", "a b c"));
        writer.addDocument(doc("3", "a b c d"));

        writer.forceMerge(4);
        IndexReader indexReader = writer.getReader();
        writer.close();
        ColFac col;

        col = new ColFac(indexReader, sort(byTxt, byTxtDesc), limit(1,2));
        col.check(q(sid("a"), sid("b")), docs(0, 2 /* id=5 */, 3 /* id=3 */));

        col = new ColFac(indexReader, sort(byTxt, byTxt), limit(1,2));
        col.check(q(sid("a"), sid("c")), docs(0, 3 /* id=3 */, 2 /* id=5 */));

        indexReader.close();
        index.close();
    }

    public void testCompareBottomWithStringVal() throws Exception {
        Directory index = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), index,
                newIndexWriterConfig(new MockAnalyzer(random())).setSimilarity(similarity).setMergePolicy(newLogMergePolicy()));

        writer.addDocument(doc("0", "a"));
        writer.addDocument(doc("1", "a b"));
        writer.commit();
        writer.addDocument(doc("5", "a b c"));
        writer.addDocument(doc("3", "a b c d"));

        writer.forceMerge(4);
        IndexReader indexReader = writer.getReader();
        writer.close();
        ColFac col;

        col = new ColFac(indexReader, sort(byTxtVal, byTxtValDesc), limit(1,2));
        col.check(q(sid("a"), sid("b")), docs(0, 2 /* id=5 */, 3 /* id=3 */));

        col = new ColFac(indexReader, sort(byTxtVal, byTxtVal), limit(1,2));
        col.check(q(sid("a"), sid("c")), docs(0, 3 /* id=3 */, 2 /* id=5 */));

        indexReader.close();
        index.close();
    }

    public void testCompareBottomWithFloat() throws Exception {
        Directory index = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), index,
                newIndexWriterConfig(new MockAnalyzer(random())).setSimilarity(similarity).setMergePolicy(newLogMergePolicy()));

        writer.addDocument(doc("0", "a"));
        writer.addDocument(doc("1", "a b"));
        writer.commit();
        writer.addDocument(doc("5", "a b c"));
        writer.addDocument(doc("3", "a b c d"));

        writer.forceMerge(4);
        IndexReader indexReader = writer.getReader();
        writer.close();
        ColFac col;

        col = new ColFac(indexReader, sort(byFloat, byFloatDesc), limit(1,2));
        col.check(q(sid("a"), sid("b")), docs(0, 2 /* id=5 */, 3 /* id=3 */));

        //col = new ColFac(indexReader, sort(byFloat, byFloat), limit(1,2));
        //col.check(q(sid("a"), sid("c")), docs(0, 3 /* id=3 */, 2 /* id=5 */));

        indexReader.close();
        index.close();
    }



    // TODO test max scores and normalization

    public void testSkipToFirsttimeMiss() throws IOException {
        final EqDisjunctionMaxQuery dq = new EqDisjunctionMaxQuery(Arrays.asList(tq("id", "1")));

        QueryUtils.check(random(), dq, indexSearcher);
        assertTrue(indexSearcher.getTopReaderContext() instanceof LeafReaderContext);
        final Weight dw = indexSearcher.createWeight(indexSearcher.rewrite(dq), ScoreMode.COMPLETE, 1);
        LeafReaderContext context = (LeafReaderContext) indexSearcher.getTopReaderContext();
        final Scorer ds = dw.scorer(context);
        final boolean skipOk = ds.iterator().advance(3) != DocIdSetIterator.NO_MORE_DOCS;
        if (skipOk) {
            fail("firsttime skipTo found a match? ... " + indexReader.document(ds.docID()).get("id"));
        }
    }

    public void testSimpleEqualScores1() throws Exception {
        EqDisjunctionMaxQuery q = new EqDisjunctionMaxQuery(Arrays.asList(tq("title", "title"), tq("text", "text")));
        // QueryUtils.check(random(), q, indexSearcher);

        ScoreDoc[] h = indexSearcher.search(q, 1000).scoreDocs;

        try {
            assertEquals("all docs should match " + q.toString(), 4, h.length);

            float score = h[0].score;
            for (int i = 1; i < h.length; i++) {
                assertEquals("score #" + i + " is not the same", score, h[i].score, SCORE_COMP_THRESH);
            }
        } catch (Error e) {
            printHits("testSimpleEqualScores1", h, indexSearcher);
            throw e;
        }

    }

    @Test
    @Ignore
    public void testSimpleEqualScores2() throws Exception {

        EqDisjunctionMaxQuery q = new EqDisjunctionMaxQuery(Arrays.asList(tq("dek", "albino"), tq("dek", "elephant")));
        QueryUtils.check(random(), q, indexSearcher);

        ScoreDoc[] h = indexSearcher.search(q, 1000).scoreDocs;

        try {
            assertEquals("3 docs should match " + q.toString(), 3, h.length);
            float score = h[0].score;
            for (int i = 1; i < h.length; i++) {
                assertEquals("score #" + i + " is not the same", score, h[i].score, SCORE_COMP_THRESH);
            }
        } catch (Error e) {
            printHits("testSimpleEqualScores2", h, indexSearcher);
            throw e;
        }

    }

    @Test
    @Ignore
    public void testSimpleEqualScores3() throws Exception {

        EqDisjunctionMaxQuery q = new EqDisjunctionMaxQuery(
                Arrays.asList(tq("hed", "albino"), tq("hed", "elephant"), tq("dek", "albino"), tq("dek", "elephant")));
        QueryUtils.check(random(), q, indexSearcher);

        ScoreDoc[] h = indexSearcher.search(q, 1000).scoreDocs;

        try {
            assertEquals("all docs should match " + q.toString(), 4, h.length);
            float score = h[0].score;
            for (int i = 1; i < h.length; i++) {
                assertEquals("score #" + i + " is not the same", score, h[i].score, SCORE_COMP_THRESH);
            }
        } catch (Error e) {
            printHits("testSimpleEqualScores3", h, indexSearcher);
            throw e;
        }

    }

    @Test
    @Ignore
    public void testBooleanRequiredEqualScores() throws Exception {

        BooleanQuery.Builder q = new BooleanQuery.Builder();
        {
            EqDisjunctionMaxQuery q1 = new EqDisjunctionMaxQuery(Arrays.asList(tq("hed", "albino"), tq("dek", "albino")));
            q.add(q1, BooleanClause.Occur.MUST);// true,false);
            QueryUtils.check(random(), q1, indexSearcher);

        }
        {
            EqDisjunctionMaxQuery q2 = new EqDisjunctionMaxQuery(Arrays.asList(tq("hed", "elephant"), tq("dek", "elephant")));
            q.add(q2, BooleanClause.Occur.MUST);// true,false);
            QueryUtils.check(random(), q2, indexSearcher);
        }

        QueryUtils.check(random(), q.build(), indexSearcher);

        ScoreDoc[] h = indexSearcher.search(q.build(), 1000).scoreDocs;

        try {
            assertEquals("3 docs should match " + q.toString(), 3, h.length);
            float score = h[0].score;
            for (int i = 1; i < h.length; i++) {
                assertEquals("score #" + i + " is not the same", score, h[i].score, SCORE_COMP_THRESH);
            }
        } catch (Error e) {
            printHits("testBooleanRequiredEqualScores1", h, indexSearcher);
            throw e;
        }
    }

    public void testBooleanSpanQuery() throws Exception {
        int hits = 0;
        Directory directory = newDirectory();
        Analyzer indexerAnalyzer = new MockAnalyzer(random());

        IndexWriterConfig config = new IndexWriterConfig(indexerAnalyzer);
        IndexWriter writer = new IndexWriter(directory, config);
        String FIELD = "content";
        Document d = new Document();
        d.add(new TextField(FIELD, "clockwork orange", Field.Store.YES));
        writer.addDocument(d);
        writer.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher searcher = newSearcher(indexReader);

        EqDisjunctionMaxQuery query = new EqDisjunctionMaxQuery(
                Arrays.asList(new SpanTermQuery(new Term(FIELD, "clockwork")), new SpanTermQuery(new Term(FIELD, "clckwork"))));
        TopScoreDocCollector collector = TopScoreDocCollector.create(1000, 1000);
        searcher.search(query, collector);
        hits = collector.topDocs().scoreDocs.length;
        for (ScoreDoc scoreDoc : collector.topDocs().scoreDocs) {
            //System.out.println(scoreDoc.doc);
        }
        indexReader.close();
        assertEquals(hits, 1);
        directory.close();
    }

    @Test
    @Ignore
    public void testNegativeScore() throws Exception {
        EqDisjunctionMaxQuery q = new EqDisjunctionMaxQuery(
                Arrays.asList(new BoostQuery(tq("hed", "albino"), -1f), new BoostQuery(tq("hed", "elephant"), -1f)));

        ScoreDoc[] h = indexSearcher.search(q, 1000).scoreDocs;

        assertEquals("all docs should match " + q.toString(), 4, h.length);

        for (int i = 0; i < h.length; i++) {
            assertTrue("score should be negative", h[i].score < 0);
        }
    }

    @Test
    @Ignore
    public void testRewriteBoolean() throws Exception {
        Query sub1 = tq("hed", "albino");
        Query sub2 = tq("hed", "elephant");
        EqDisjunctionMaxQuery q = new EqDisjunctionMaxQuery(Arrays.asList(sub1, sub2));
        Query rewritten = indexSearcher.rewrite(q);
        assertTrue(rewritten instanceof BooleanQuery);
        BooleanQuery bq = (BooleanQuery) rewritten;
        assertEquals(bq.clauses().size(), 2);
        assertEquals(bq.clauses().get(0), new BooleanClause(sub1, BooleanClause.Occur.SHOULD));
        assertEquals(bq.clauses().get(1), new BooleanClause(sub2, BooleanClause.Occur.SHOULD));
    }

    public static Query tq(String f, String t) {
        return new TermQuery(new Term(f, t));
    }

    public static Query tq(String ft) {
        int cln = ft.indexOf(':');
        return new TermQuery(new Term(ft.substring(0, cln), ft.substring(cln + 1)));
    }

    public static Query tq(String f, String t, float b) {
        return new BoostQuery(tq(f, t), b);
    }

    protected void printHits(String test, ScoreDoc[] h, IndexSearcher searcher) throws Exception {

        System.err.println("------- " + test + " -------");

        DecimalFormat f = new DecimalFormat("0.000000000", DecimalFormatSymbols.getInstance(Locale.ROOT));

        for (int i = 0; i < h.length; i++) {
            Document d = searcher.doc(h[i].doc);
            float score = h[i].score;
            System.err.println("#" + i + ": " + f.format(score) + " - " + d.get("id"));
        }
    }
}
