package code.ponfee.es;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.junit.Test;

public class TestEsClient extends BaseTest<ElasticSearchClient> {

    @Test
    public void test1() {
        SearchRequestBuilder search = getBean().prepareSearch("ddt_risk_wastaged", "wastaged_city_es");
        consoleJson(getBean().rankingSearch(search, 10));
    }
}
