package org.apache.solr.search.eq;

import static org.apache.solr.search.SortSpecParsing.DOCID;
import static org.apache.solr.search.SortSpecParsing.SCORE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.EqSortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QueryCommand;

public class EqQueryCommand extends QueryCommand {

    public static final String SUB_QUERY_SORT_PREFIX = "sort_";
    public static final String SUB_QUERY_LIMIT_PREFIX = "lim_";

    private QueryCommand originCmd;

    private Map<String, Sort> subQuerySorts;
    private Map<String, Integer> subQueryLimits;

    public EqQueryCommand(QueryCommand cmd, SolrParams params, IndexSchema indexSchema) {
        this.originCmd = cmd;
        this.setSubQuerySorts(getSubQuerySorts(params, indexSchema));
        this.setSubQueryLimits(getSubQueryLimits(params));
    }

    public QueryCommand getOriginCmd() {
        return originCmd;
    }

    public void setOriginCmd(QueryCommand originCmd) {
        this.originCmd = originCmd;
    }

    public Map<String, Sort> getSubQuerySorts() {
        return subQuerySorts;
    }

    public QueryCommand setSubQuerySorts(Map<String, Sort> subQuerySorts) {
        this.subQuerySorts = subQuerySorts;
        return this;
    }

    public Map<String, Integer> getSubQueryLimits() {
        return subQueryLimits;
    }

    public QueryCommand setSubQueryLimits(Map<String, Integer> subQueryLimits) {
        this.subQueryLimits = subQueryLimits;
        return this;
    }

    private Map<String, Sort> getSubQuerySorts(SolrParams params, IndexSchema schema) {
        Map<String, Sort> subQSorts = new HashMap<>();

        for (Map.Entry<String, String[]> entry : params) {
            if (entry.getKey().contains(SUB_QUERY_SORT_PREFIX)) {
                List<EqSortField> sorts = new ArrayList<>();
                String subQueryName = entry.getKey();
                String[] fields = entry.getValue()[0].split(",");

                for (String s : fields) {
                    boolean top = s.contains("asc") ? false : true;
                    String field = s.trim().split("\\s+")[0];

                    if (SCORE.equals(field)) {
                        if (top) {
                            sorts.add(EqSortField.FIELD_SCORE);
                        } else {
                            sorts.add(new EqSortField(null, EqSortField.Type.SCORE, true));
                        }
                    } else if (DOCID.equals(field)) {
                        sorts.add(new EqSortField(null, EqSortField.Type.DOC, top));
                    } else {
                        // try to find the field
                        SchemaField sf = schema.getFieldOrNull(field);
                        if (null == sf) {
                            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "sort param field can't be found: " + field);
                        }
                        sorts.add(new EqSortField(sf.getSortField(top)));
                    }
                }

                subQSorts.put(subQueryName, new Sort(sorts.toArray(new EqSortField[sorts.size()])));
            }
        }

        return subQSorts;
    }

    private Map<String, Integer> getSubQueryLimits(SolrParams params) {
        Map<String, Integer> subqLimits = new HashMap<>();

        for (Map.Entry<String, String[]> entry : params) {
            if (entry.getKey().contains(SUB_QUERY_LIMIT_PREFIX)) {
                subqLimits.put(entry.getKey(), Integer.parseInt(entry.getValue()[0].trim()));
            }
        }

        return subqLimits;
    }

}
