package code.ponfee.es;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 * 查询条件构建
 * @author fupf
 */
public class ESQueryBuilder {

    private final String[] indices; // 索引
    private final String[] types; // 类型
    private BoolQueryBuilder boolQuery; // bool筛选条件
    private String[] fields; // 查询的字段
    private final List<SortBuilder<?>> sorts = new ArrayList<>(1); // 排序
    private final List<AggregationBuilder> aggs = new ArrayList<>(1); // 分组聚合

    private ESQueryBuilder(String[] indices, String[] types) {
        this.indices = indices;
        this.types = types;
    }

    public static ESQueryBuilder newBuilder(String index, String... type) {
        return new ESQueryBuilder(new String[] { index }, type);
    }

    public static ESQueryBuilder newBuilder(String[] indices, String... types) {
        return new ESQueryBuilder(indices, types);
    }

    public ESQueryBuilder fields(String... fields) {
        if (this.fields != null) {
            throw new UnsupportedOperationException("Cannot repeat set fields.");
        }
        this.fields = fields;
        return this;
    }

    // -----------The clause (query) must appear in matching documents，所有term全部匹配（AND）--------- //
    /**
     * ?=term
     * @param name
     * @param term
     * @return
     */
    public ESQueryBuilder mustEquals(String name, Object term) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.must(QueryBuilders.termQuery(name, term));
        return this;
    }

    /**
     * ? IN(item1,item2,..,itemn)
     * @param name
     * @param items  
     * @return
     */
    public <T> ESQueryBuilder mustIn(String name, List<T> items) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.must(QueryBuilders.termsQuery(name, items));
        return this;
    }

    /**
     * ? IN(item1,item2,..,itemn)
     * @param name
     * @param items  
     * @return
     */
    public ESQueryBuilder mustIn(String name, Object... items) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.must(QueryBuilders.termsQuery(name, items));
        return this;
    }

    /**
     * must range query
     * ? BETWEEN from AND to
     * @param name
     * @param from
     * @param to
     * @return
     */
    public ESQueryBuilder mustRange(String name, Object from, Object to) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.must(QueryBuilders.rangeQuery(name).from(from).to(to));
        return this;
    }

    /**
     * EXISTS(name) && name IS NOT NULL && name IS NOT EMPTY
     * @param name
     * @return
     */
    public ESQueryBuilder mustExists(String name) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.must(QueryBuilders.existsQuery(name));
        return this;
    }

    /**
     * NOT EXISTS(name) && name IS NULL && name IS EMPTY
     * @param name
     * @return
     */
    public ESQueryBuilder mustNotExists(String name) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.mustNot(QueryBuilders.existsQuery(name));
        return this;
    }

    /**
     * name LIKE 'prefix%'
     * @param name
     * @param prefix
     * @return
     */
    public ESQueryBuilder mustPrefix(String name, String prefix) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.must(QueryBuilders.prefixQuery(name, prefix));
        return this;
    }

    /**
     * regexp query
     * @param name
     * @param regexp
     * @return
     */
    public ESQueryBuilder mustRegexp(String name, String regexp) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.must(QueryBuilders.regexpQuery(name, regexp));
        return this;
    }

    /**
     * like query
     * name LIKE '*wildcard*'
     * @param name
     * @param wildcard
     * @return
     */
    public ESQueryBuilder mustLike(String name, String wildcard) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.must(QueryBuilders.wildcardQuery(name, wildcard));
        return this;
    }

    /**
     * not like
     * @param name  before or after or both here with *
     * @param wildcard
     * @return
     */
    public ESQueryBuilder mustNotLike(String name, String wildcard) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.mustNot(QueryBuilders.wildcardQuery(name, wildcard));
        return this;
    }

    /**
     * ? !=term
     * @param name
     * @param term
     * @return
     */
    public ESQueryBuilder mustNotEquals(String name, Object term) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.mustNot(QueryBuilders.termQuery(name, term));
        return this;
    }

    /**
     * ? NOT IN(item1,item2,..,itemn)
     * @param name
     * @param items
     * @return
     */
    public ESQueryBuilder mustNotIn(String name, Object... items) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.mustNot(QueryBuilders.termsQuery(name, items));
        return this;
    }

    /**
     * !(BETWEEN from AND to)
     * @param name
     * @param from
     * @param to
     * @return
     */
    public ESQueryBuilder mustNotRange(String name, Object from, Object to) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.mustNot(QueryBuilders.rangeQuery(name).from(from).to(to));
        return this;
    }

    /**
     * name NOT LIKE 'prefix%'
     * @param name
     * @param prefix
     * @return
     */
    public ESQueryBuilder mustNotPrefix(String name, String prefix) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.mustNot(QueryBuilders.prefixQuery(name, prefix));
        return this;
    }

    // --------------The clause (query) should appear in the matching document. ------------- //
    // --------------In a boolean query with no must clauses, one or more should clauses must match a document. ------------- //
    // --------------The minimum number of should clauses to match can be set using the minimum_should_matchparameter. ------------- //
    // --------------至少有一个term匹配（OR） ------------- //
    /**
     * ?=text
     * @param name
     * @param term 
     * @return
     */
    public ESQueryBuilder shouldEquals(String name, Object term) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.should(QueryBuilders.termQuery(name, term));
        return this;
    }

    /**
     * ? IN(item1,item2,..,itemn)
     * @param name
     * @param items
     * @return
     */
    public <T> ESQueryBuilder shouldIn(String name, List<T> items) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.should(QueryBuilders.termsQuery(name, items));
        return this;
    }

    /**
     * ? IN(item1,item2,..,itemn)
     * @param name
     * @param items
     * @return
     */
    public ESQueryBuilder shouldIn(String name, Object... items) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.should(QueryBuilders.termsQuery(name, items));
        return this;
    }

    /**
     * should range query
     * ? BETWEEN from AND to
     * @param name
     * @param from
     * @param to
     * @return
     */
    public ESQueryBuilder shouldRange(String name, Object from, Object to) {
        if (this.boolQuery == null) {
            this.boolQuery = QueryBuilders.boolQuery();
        }
        this.boolQuery.should(QueryBuilders.rangeQuery(name).from(from).to(to));
        return this;
    }

    // --------------------------------------聚合函数-------------------------------- //
    /**
     * 聚合
     * @param agg
     * @return
     */
    public ESQueryBuilder aggs(AggregationBuilder agg) {
        this.aggs.add(agg);
        return this;
    }

    // --------------------------------------排序-------------------------------- //
    /**
     * ORDER BY sort ASC
     * @param name
     * @return
     */
    public ESQueryBuilder asc(String name) {
        this.sorts.add(SortBuilders.fieldSort(name).order(SortOrder.ASC));
        return this;
    }

    /**
     * ORDER BY sort DESC
     * @param name
     * @return
     */
    public ESQueryBuilder desc(String name) {
        this.sorts.add(SortBuilders.fieldSort(name).order(SortOrder.DESC));
        return this;
    }

    // --------------------------package methods-------------------------
    SearchResponse pagination(TransportClient client, int from, int size) {
        SearchRequestBuilder search = build(client, size);
        search.setSearchType(SearchType.DFS_QUERY_THEN_FETCH); // 深度分布
        search.setFrom(from).setExplain(false);
        return search.get();
    }

    SearchResponse scroll(TransportClient client, int size) {
        SearchRequestBuilder search = build(client, size);
        search.setScroll(ElasticSearchClient.SCROLL_TIMEOUT);
        //search.setSearchType(SearchType.QUERY_THEN_FETCH); // default QUERY_THEN_FETCH
        return search.get();
    }

    Aggregations aggregation(TransportClient client) {
        SearchRequestBuilder search = build(client, 0);
        search.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        return search.get().getAggregations();
    }

    // --------------------------private methods-------------------------
    private SearchRequestBuilder build(TransportClient client, int size) {
        SearchRequestBuilder search = client.prepareSearch(indices);
        if (types != null) {
            search.setTypes(types);
        }
        if (fields != null) {
            search.setFetchSource(fields, null);
        }
        if (boolQuery != null) {
            search.setQuery(boolQuery);
        }
        for (SortBuilder<?> sort : sorts) {
            search.addSort(sort);
        }
        for (AggregationBuilder agg : aggs) {
            search.addAggregation(agg);
        }
        return search.setSize(size);
    }

    public String dsl(int from, int size) {
        SearchSourceBuilder search = new SearchSourceBuilder();
        if (fields != null) {
            search.fetchSource(fields, null);
        }
        if (boolQuery != null) {
            search.query(boolQuery);
        }
        for (SortBuilder<?> sort : sorts) {
            search.sort(sort);
        }
        for (AggregationBuilder agg : aggs) {
            search.aggregation(agg);
        }
        search.from(from);
        search.size(size);

        return search.toString();
    }

}
