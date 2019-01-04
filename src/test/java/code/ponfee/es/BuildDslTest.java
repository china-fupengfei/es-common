package code.ponfee.es;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Test;

public class BuildDslTest {

    private final ElasticSearchClient esClient;

    public BuildDslTest() {
        esClient = new ElasticSearchClient("es-cluster", "127.0.0.1:9300");
    }

    @Test
    public void test1() {
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                                              .must(QueryBuilders.existsQuery("aa"))
                                              .mustNot(QueryBuilders.termQuery("bb", ""));
        System.out.println(query.toString());

        SearchRequestBuilder search = esClient.prepareSearch("index", "type")
                                              .setFrom(0).setSize(10).setQuery(query);
        System.out.println(search.toString());
    }

    @Test
    public void test2() {
        ESQueryBuilder query = ESQueryBuilder.newBuilder("index", "type");
        query.mustExists("aa").mustNotEquals("bb", "");
        System.out.println(query.dsl(0, 10));
    }
}
