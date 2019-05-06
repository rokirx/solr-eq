package org.apache.solr.handler.component.eq;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEqShardsMerging extends SolrCloudTestCase {
    private static final String COLLECTION = "eq_collection";

    private static final int numShards = 2;
    private static final int numReplicas = 1;
    private static final int maxShardsPerNode = 2;
    private static final int nodeCount = (numShards * numReplicas + (maxShardsPerNode - 1)) / maxShardsPerNode;

    private static final String id = "id";

    @BeforeClass
    public static void setupCluster() throws Exception {
        URL configsetUrl = TestEqQueryComponent.class.getResource("/solr/configset");
        Path configsetPath = Paths.get(configsetUrl.toURI());

        // create and configure cluster
        configureCluster(nodeCount).addConfig("conf", configsetPath).configure();

        // create an empty collection
        CollectionAdminRequest.createCollection(COLLECTION, "conf", numShards, numReplicas).setMaxShardsPerNode(maxShardsPerNode)
                .setRouterName("implicit").setRouterField("shard").setShards("shard-A,shard-B").process(cluster.getSolrClient());

        // CloudSolrClient cloudSolrClient = cluster.getSolrClient();
        // Collection<Slice> slices =
        // cloudSolrClient.getZkStateReader().getClusterState().getCollection(COLLECTION).getSlices();
        SolrInputDocument docA = new SolrInputDocument(id, "a", "shard", "shard-A", "title_str", "title a", "text_ws", "text a", "date_dt",
                "2019-01-01T00:00:00Z");
        SolrInputDocument docB = new SolrInputDocument(id, "b", "shard", "shard-B", "title_str", "title b", "text_ws", "text b", "date_dt",
                "2019-01-02T00:00:00Z");

        SolrInputDocument docC = new SolrInputDocument(id, "c", "shard", "shard-A", "title_str", "title c", "text_ws", "text c", "date_dt",
                "2019-01-03T00:00:00Z");
        SolrInputDocument docD = new SolrInputDocument(id, "d", "shard", "shard-B", "title_str", "title d", "text_ws", "text d", "date_dt",
                "2019-01-04T00:00:00Z");

        SolrInputDocument docE = new SolrInputDocument(id, "e", "shard", "shard-A", "title_str", "title e", "text_ws", "text e", "date_dt",
                "2019-01-05T00:00:00Z");

        new UpdateRequest().add(docA).add(docB).add(docC).add(docD).add(docE).commit(cluster.getSolrClient(), COLLECTION);
    }

    @Test
    public void testEqShardsMerging0() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "id:a << id:b");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(2, rsp1.getResults().size());
        assertEquals("a", rsp1.getResults().get(0).getFieldValue("id"));

        SolrQuery solrQuery2 = new SolrQuery("q", "id:b << id:a");
        solrQuery2.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery2.setRequestHandler("/eq_search");
        QueryResponse rsp2 = cloudSolrClient.query(COLLECTION, solrQuery2);

        assertEquals(2, rsp2.getResults().size());
        assertEquals("b", rsp2.getResults().get(0).getFieldValue("id"));

        SolrQuery solrQuery3 = new SolrQuery("q", "(id:a id:b) << id:b");
        solrQuery3.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery3.setRequestHandler("/eq_search");
        QueryResponse rsp3 = cloudSolrClient.query(COLLECTION, solrQuery3);

        assertEquals(2, rsp3.getResults().size());
        assertEquals("a", rsp3.getResults().get(0).getFieldValue("id"));
        assertEquals("b", rsp3.getResults().get(1).getFieldValue("id"));

        SolrQuery solrQuery4 = new SolrQuery("q", "id:b << (id:a id:b)");
        solrQuery4.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery4.setRequestHandler("/eq_search");
        QueryResponse rsp4 = cloudSolrClient.query(COLLECTION, solrQuery4);

        assertEquals(2, rsp4.getResults().size());
        assertEquals("b", rsp4.getResults().get(0).getFieldValue("id"));
        assertEquals("a", rsp4.getResults().get(1).getFieldValue("id"));
    }

    @Test
    public void testEqShardsMerging1() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "title_str:\"title a\" << id:b");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(2, rsp1.getResults().size());
        assertEquals("a", rsp1.getResults().get(0).getFieldValue("id"));
    }

    @Test
    public void testEqShardsMerging2() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "text_ws:text << id:b");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(5, rsp1.getResults().size());
        assertEquals(1, rsp1.getResults().get(0).getFieldValue("sqidx"));
    }

    @Test
    public void testEqShardsMergingSort3() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "text_ws:text << id:b");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery1.addSort("date_dt", ORDER.desc);
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(5, rsp1.getResults().size());
        assertEquals("e", rsp1.getResults().get(0).getFieldValue("id"));
    }

    @Test
    public void testEqShardsMergingSort4() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "text_ws:text << id:b");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery1.add("sort_0", "date_dt desc");
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(5, rsp1.getResults().size());
        assertEquals("e", rsp1.getResults().get(0).getFieldValue("id"));
        assertEquals("d", rsp1.getResults().get(1).getFieldValue("id"));
        assertEquals("c", rsp1.getResults().get(2).getFieldValue("id"));
        assertEquals("b", rsp1.getResults().get(3).getFieldValue("id"));
        assertEquals("a", rsp1.getResults().get(4).getFieldValue("id"));
    }

    @Test
    public void testEqShardsMergingSort5() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "(id:a id:b) << (id:c id:d)");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery1.add("sort_0", "date_dt desc");
        solrQuery1.add("sort_1", "date_dt desc");
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(4, rsp1.getResults().size());
        assertEquals("b", rsp1.getResults().get(0).getFieldValue("id"));
        assertEquals("a", rsp1.getResults().get(1).getFieldValue("id"));
        assertEquals("d", rsp1.getResults().get(2).getFieldValue("id"));
        assertEquals("c", rsp1.getResults().get(3).getFieldValue("id"));
    }

    @Test
    public void testEqShardsMergingSortLimit6() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "(id:a id:b) << (id:c id:d)");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery1.add("sort_0", "date_dt desc");
        solrQuery1.add("lim_0", "1");
        solrQuery1.add("sort_1", "date_dt desc");
        solrQuery1.add("lim_1", "1");
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(2, rsp1.getResults().size());
        assertEquals("b", rsp1.getResults().get(0).getFieldValue("id"));
        assertEquals("d", rsp1.getResults().get(1).getFieldValue("id"));
    }

    @Test
    public void testEqShardsMergingSortLimit7() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "text_ws:text << id:b");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery1.addSort("date_dt", ORDER.desc);
        solrQuery1.add("lim_0", "2");
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(2, rsp1.getResults().size());
        assertEquals("e", rsp1.getResults().get(0).getFieldValue("id"));
        assertEquals("d", rsp1.getResults().get(1).getFieldValue("id"));
    }

    @Test
    public void testEqShardsMergingSortLimit8() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "(id:a id:b) << (id:c id:d)");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,subqRoots,score");
        solrQuery1.add("sort_0", "date_dt desc");
        solrQuery1.add("lim_0", "1");
        solrQuery1.add("sort_1", "date_dt desc");
        solrQuery1.add("lim_1", "2");
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(3, rsp1.getResults().size());
        assertEquals("b", rsp1.getResults().get(0).getFieldValue("id"));
        assertEquals("d", rsp1.getResults().get(1).getFieldValue("id"));
        assertEquals("c", rsp1.getResults().get(2).getFieldValue("id"));
    }

    @Test
    public void testEqShardsMerging9() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "text_ws:text << id:a");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,score");
        solrQuery1.set(CommonParams.ROWS, "2");
        solrQuery1.addSort("id", ORDER.asc);
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(2, rsp1.getResults().size());
        assertEquals("a", rsp1.getResults().get(0).getFieldValue("id"));
        assertEquals("b", rsp1.getResults().get(1).getFieldValue("id"));
    }
    
    @Test
    public void testEqShardsMergingSortLimit10() throws Exception {
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();

        SolrQuery solrQuery1 = new SolrQuery("q", "text_ws:text << id:a");
        solrQuery1.set(CommonParams.FL, "[shard],id,sqidx,score");
        solrQuery1.set(CommonParams.ROWS, "2");
        solrQuery1.add("sort_0", "date_dt desc");
        solrQuery1.add("lim_0", "1");
        solrQuery1.add("sort_1", "date_dt desc");
        solrQuery1.add("lim_1", "2");
        solrQuery1.addSort("id", ORDER.asc);
        solrQuery1.setRequestHandler("/eq_search");
        QueryResponse rsp1 = cloudSolrClient.query(COLLECTION, solrQuery1);

        assertEquals(2, rsp1.getResults().size());
        assertEquals("e", rsp1.getResults().get(0).getFieldValue("id"));
        assertEquals("a", rsp1.getResults().get(1).getFieldValue("id"));
    }
}
