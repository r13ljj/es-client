package com.jonex.es.client.api;

import com.jonex.es.client.NodeClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;

/**
 * <pre>
 *
 *  File: QueryApi.java
 *
 *  Copyright (c) 2018, globalegrow.com All Rights Reserved.
 *
 *  Description:
 *  TODO
 *
 *  Revision History
 *  Date,					Who,					What;
 *  2018/3/23				lijunjun				Initial.
 *
 * </pre>
 */
public class QueryApi {

    public void matchAllQuery(){
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .setQuery(QueryBuilders.matchAllQuery())
                .execute()
                .actionGet();
    }

    public void matchQuery(){
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .setQuery(QueryBuilders.matchQuery("name", "james"))
                .execute()
                .actionGet();
    }

    public void multiMatchQuery(){
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .setQuery(QueryBuilders.multiMatchQuery("jonex","username", "alias"))
                .execute()
                .actionGet();
    }

    public void commonTermsQuery(){
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .setQuery(QueryBuilders.commonTermsQuery("name","kimchy"))
                .execute()
                .actionGet();
    }

    public void queryStringQuery(){
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .setQuery(QueryBuilders.queryStringQuery("elasticsearch bigdata ai"))
                .execute()
                .actionGet();
    }

    public void simpleueryStringQuery(){
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .setQuery(QueryBuilders.simpleQueryStringQuery("+elasticsearch +bigdata -ai"))
                .execute()
                .actionGet();
    }

    public void termQuery(){
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .setQuery(QueryBuilders.termQuery("name", "jonex"))
                .execute()
                .actionGet();
    }

    public void termsQuery(){
        //termsQuery("tags", "blue", "pill");
    }
    public void rangeQuery(){
        /*rangeQuery("price")
                .from(5)
                .to(10)
                .includeLower(true)
                .includeUpper(false)*/
    }
    public void existsQuery(){
        //existsQuery("name");
    }
    public void prefixQuery(){
        //prefixQuery("brand", "heine");
    }
    public void wildcardQuery(){
        //wildcardQuery("user","k?mch*");
    }
    public void regexpQuery(){
        //regexpQuery("name.first", "s.*y");
    }
    public void fuzzyQuery(){
        //fuzzyQuery("name", "kimchy");
    }
    public void typeQuery(){
        //typeQuery("my_type");
    }

    public void idsQuery(){
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .setQuery(QueryBuilders.idsQuery("1", "2", "3"))
                .execute()
                .actionGet();
    }

    public void constantScoreQuery(){
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .setQuery(QueryBuilders.constantScoreQuery(
                            QueryBuilders.termQuery("name","kimchy")).boost(2.0f))
                .execute()
                .actionGet();
    }

    public void boolQuery(){
        /*boolQuery()
                .must(termQuery("content", "test1"))
                .must(termQuery("content", "test4"))
                .mustNot(termQuery("content", "test2"))
                .should(termQuery("content", "test3"))
                .filter(termQuery("content", "test5"));*/
    }
    public void disMaxQuery(){
        /*disMaxQuery()
                .add(termQuery("name", "kimchy"))
                .add(termQuery("name", "elasticsearch"))
                .boost(1.2f)
                .tieBreaker(0.7f);*/
    }

    public void functionScoreQuery(){
        /*FunctionScoreQueryBuilder.FilterFunctionBuilder[] functions = {
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        QueryBuilders.matchQuery("name", "kimchy"),
                        randomFunction()),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        exponentialDecayFunction("age", 0L, 1L))
        };
        functionScoreQuery(functions);*/
    }
    public void boostingQuery(){
        /*boostingQuery(
                termQuery("name","kimchy"),
                termQuery("name","dadoonet"))
                .negativeBoost(0.2f);*/
    }

    public void nestedQuery(){
        /*nestedQuery(
                "obj1",
                boolQuery()
                        .must(matchQuery("obj1.name", "blue"))
                        .must(rangeQuery("obj1.count").gt(5)),
                ScoreMode.Avg);*/
    }
    public void hasChildQuery(){
        /*JoinQueryBuilders.hasChildQuery(
                "blog_tag",
                termQuery("tag","something"),
                ScoreMode.None);*/
    }
    public void hasParentQuery(){
        /*JoinQueryBuilders.hasParentQuery(
                "blog",
                termQuery("tag","something"),
                false);*/
    }

    public void geoShapeQuery(){
        //TODO
    }
    public void geoBoundingBoxQuery(){
        //TODO
    }
    public void geoDistanceQuery(){
        //TODO
    }
    public void geoPolygonQuery(){
        //TODO
    }

    public void moreLikeThisQuery(){
        /*String[] fields = {"name.first", "name.last"};
        String[] texts = {"text like this one"};

        moreLikeThisQuery(fields, texts, null)
                .minTermFreq(1)
                .maxQueryTerms(12);*/
    }

    public void scriptQuery(){
        /*Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", 5);
        scriptQuery(new Script(
                ScriptType.STORED,
                null,
                "myscript",
                singletonMap("param1", 5)));*/
    }

    public void percolateQuery(){
        /*// create an index with a percolator field with the name 'query':
        client.admin().indices().prepareCreate("myIndexName")
                .addMapping("query", "query", "type=percolator")
                .addMapping("docs", "content", "type=text")
                .get();

        //This is the query we're registering in the percolator
        QueryBuilder qb = termQuery("content", "amazing");

        //Index the query = register it in the percolator
        client.prepareIndex("myIndexName", "query", "myDesignatedQueryName")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("query", qb) // Register the query
                        .endObject())
                .setRefreshPolicy(RefreshPolicy.IMMEDIATE) // Needed when the query shall be available immediately
                .get();
        //Build a document to check against the percolator
        XContentBuilder docBuilder = XContentFactory.jsonBuilder().startObject();
        docBuilder.field("content", "This is amazing!");
        docBuilder.endObject(); //End of the JSON root object

        PercolateQueryBuilder percolateQuery = new PercolateQueryBuilder("query", "docs", docBuilder.bytes());

        // Percolate, by executing the percolator query in the query dsl:
        SearchResponse response = client().prepareSearch("myIndexName")
                .setQuery(percolateQuery))
        .get();
        //Iterate over the results
        for(SearchHit hit : response.getHits()) {
            // Percolator queries as hit
        }*/
    }

    public void wrapperQuery(){
        /*String query = "{\"term\": {\"user\": \"kimchy\"}}";
        wrapperQuery(query);*/
    }

    public void spanQuery(){
        //span term query
        //span multi term query
        //span first query
        //span near query
        //span or query
        //span and query
        //span contain query
        //span within query
    }


}
