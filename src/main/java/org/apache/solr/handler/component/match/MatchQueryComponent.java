package org.apache.solr.handler.component.match;

import java.io.IOException;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.XmlConfigFile;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.util.DOMUtil;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class MatchQueryComponent extends SearchComponent implements SolrCoreAware {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    static final String CONFIG_FILE = "config-file";

    protected SolrParams initArgs;

    private final Map<String, String> matchesCache = new HashMap<String, String>();

    @Override
    public void init(@SuppressWarnings("rawtypes") NamedList args) {
        super.init(args);
        this.initArgs = args.toSolrParams();
    }

    @Override
    public void inform(SolrCore core) {
        try {
            loadConfiguration(core);
        } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error initializing " + MatchQueryComponent.class.getSimpleName(), e);
        }
    }

    private void loadConfiguration(SolrCore core)
            throws KeeperException, InterruptedException, ParserConfigurationException, IOException, SAXException {
        synchronized (matchesCache) {
            matchesCache.clear();
            if (initArgs != null) {
                log.info("Initializing MatchQueryComponent");
                XPath xpath = XPathFactory.newInstance().newXPath();
                String configFileName = initArgs.get(CONFIG_FILE);
                XmlConfigFile cfg = new XmlConfigFile(core.getResourceLoader(), configFileName, null, null);
                NodeList nodes = (NodeList) cfg.evaluate("matches/match", XPathConstants.NODESET);
                for (int i = 0; i < nodes.getLength(); i++) {
                    Node node = nodes.item(i);
                    String listFile = DOMUtil.getAttr(node, "list", "missing match 'list'");
                    List<String> matchesList = core.getResourceLoader().getLines(listFile);
                    NodeList children;
                    try {
                        children = (NodeList) xpath.evaluate("equery", node, XPathConstants.NODESET);
                    } catch (XPathExpressionException e) {
                        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "query requires '<equery .../>' child");
                    }
                    String matchExpr = null;
                    for (int j = 0; j < children.getLength(); j++) {
                        Node child = children.item(j);
                        matchExpr = DOMUtil.getText(child).trim();
                    }
                    if (matchExpr != null && !matchesList.isEmpty()) {
                        for (String match : matchesList) {
                            matchesCache.put(match.toLowerCase(), matchExpr.replace("$match", match).toLowerCase());
                        }
                    }
                }
            }
        }
    }

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
        String userQuery = rb.req.getParams().get(CommonParams.Q);

        Tokenizer tokenizer = new WhitespaceTokenizer();
        tokenizer.setReader(new StringReader(userQuery.toLowerCase()));
        CharTermAttribute termAttr = tokenizer.addAttribute(CharTermAttribute.class);
        tokenizer.reset();
        while (tokenizer.incrementToken()) {
            String term = termAttr.toString();
            if (matchesCache.containsKey(term)) {
                ModifiableSolrParams modifiableSolrParams = new ModifiableSolrParams(rb.req.getParams());
                modifiableSolrParams.set(CommonParams.Q, matchesCache.get(term) + " << " + userQuery);
                rb.req.setParams(modifiableSolrParams);
            }
        }
        tokenizer.close();
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
        // Do nothing -- the real work is modifying the input query
    }

    @Override
    public String getDescription() {
        return "Query Match -- move recognized query on first place";
    }

}
