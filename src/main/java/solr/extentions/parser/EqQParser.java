package solr.extentions.parser;

import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;

public class EqQParser extends QParser {

    QueryParser eqQueryParser;

    public EqQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() throws SyntaxError {
        String qstr = getString();
        if (qstr == null || qstr.length() == 0)
            return null;

        String defaultField = getParam(CommonParams.DF);
        eqQueryParser = new QueryParser(defaultField, this);

        eqQueryParser.setDefaultOperator(QueryParsing.parseOP(getParam(QueryParsing.OP)));
        eqQueryParser.setSplitOnWhitespace(StrUtils.parseBool(getParam(QueryParsing.SPLIT_ON_WHITESPACE), QueryParser.DEFAULT_SPLIT_ON_WHITESPACE));
        eqQueryParser.setAllowSubQueryParsing(true);

        return eqQueryParser.parse(qstr);
    }

}
