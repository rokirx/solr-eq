package org.apache.solr.handler.component.match;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMatchQueryComponentCoreAware extends SolrTestCaseJ4 {

    private static final String id = "id";

    @BeforeClass
    public static void beforeClass() throws Exception {
        File testHome = createTempDir().toFile();
        File coreDir = new File(testHome.getAbsolutePath() + "/eq_core");
        FileUtils.copyDirectory(new File(TestMatchQueryComponentCoreAware.class.getResource("/solr/configset").getFile()), coreDir);
        initCore("solrconfig.xml", "schema.xml", testHome.getAbsolutePath(), "eq_core");

        assertU(adoc(id, "1", "title_str", "Title 1", "text_txt", "Text 1 London", "date_dt", "2019-01-01T00:00:00Z"));
        assertU(adoc(id, "2", "title_str", "Title 2", "text_txt", "Text 2", "date_dt", "2019-01-02T00:00:00Z"));
        assertU(adoc(id, "3", "title_str", "Title 3", "text_txt", "Text 3", "date_dt", "2019-01-03T00:00:00Z"));
        assertU(adoc(id, "4", "title_str", "Title 4", "text_txt", "Text 4", "date_dt", "2019-01-04T00:00:00Z"));
        assertU(adoc(id, "5", "title_str", "Title 5", "text_txt", "Text 5 London", "date_dt", "2019-01-05T00:00:00Z"));
        assertU(commit());
    }

    @Test
    public void testCoreMatchQueryComponent() throws Exception {
        assertQ(req("q", "test london", "sort", "id asc", "qt", "/eq_search", "df", "title_str"), "//*[@numFound='2']",
                "//result/doc[1]/str[@name='id'][.='1']");
    }

}
