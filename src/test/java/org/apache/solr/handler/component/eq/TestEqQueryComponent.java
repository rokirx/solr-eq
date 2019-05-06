package org.apache.solr.handler.component.eq;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEqQueryComponent extends SolrCloudTestCase {

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
                .process(cluster.getSolrClient());

        // add a documents
        new UpdateRequest().add(id, "1", "title_str", "Title 1", "text_txt", "Text 1", "date_dt", "2019-01-01T00:00:00Z")
                .add(id, "2", "title_str", "Title 2", "text_txt", "Text 2", "date_dt", "2019-01-02T00:00:00Z")
                .add(id, "3", "title_str", "Title 3", "text_txt", "Text 3", "date_dt", "2019-01-03T00:00:00Z")
                .add(id, "4", "title_str", "Title 4", "text_txt", "Text 4", "date_dt", "2019-01-04T00:00:00Z")
                .add(id, "5", "title_str", "Title 5", "text_txt", "Text 5", "date_dt", "2019-01-05T00:00:00Z")
                .commit(cluster.getSolrClient(), COLLECTION);
    }

    @Test
    public void testEqComponent() throws Exception {
        final SolrQuery solrQuery = new SolrQuery("q", "id:5* << id:1");
        solrQuery.setRequestHandler("/eq_search");
        final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
        final QueryResponse rsp = cloudSolrClient.query(COLLECTION, solrQuery);
        assertEquals(2, rsp.getResults().size());
        assertEquals("5", rsp.getResults().get(0).getFieldValue(id));
    }

}
