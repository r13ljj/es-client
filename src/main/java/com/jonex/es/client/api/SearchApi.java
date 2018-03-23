package com.jonex.es.client.api;

import com.jonex.es.client.NodeClient;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xubai on 2018/03/20 上午1:28.
 */
public class SearchApi {


    public void prepareSearch()throws Exception{
        SearchResponse searchResponse = NodeClient.getInstance().getClient()
                .prepareSearch("twitter", "tweet")
                .setTypes("type")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("multi", "test"))
                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))
                .setFrom(0).setSize(60)
                .setExplain(true)
                .get();

        // MatchAll on the whole cluster with all default options
        SearchResponse response = NodeClient.getInstance().getClient()
                .prepareSearch()
                .get();
    }

    public void prepareSearchScroll()throws Exception{
        QueryBuilder queryBuilder = QueryBuilders.termQuery("gender", "male");
        SearchResponse scrollResponse = NodeClient.getInstance().getClient()
                .prepareSearch("persons")   //index
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(10000))
                .setQuery(queryBuilder)
                .setSize(100)   //max of 100 hits will be returned for each scroll
                .get();
        do {
            for(SearchHit searchHit : scrollResponse.getHits().getHits()){
                //handle hit doc
            }
            scrollResponse = NodeClient.getInstance().getClient()
                    .prepareSearchScroll(scrollResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
        }while (scrollResponse.getHits().getHits().length != 0);

    }

    public void multiSearch()throws Exception{
        TransportClient client = NodeClient.getInstance().getClient();
        SearchRequestBuilder srb1 = client
                .prepareSearch()
                .setQuery(QueryBuilders.queryStringQuery("elasticsearch"))
                .setSize(10);
        SearchRequestBuilder srb2 = client
                .prepareSearch()
                .setQuery(QueryBuilders.matchQuery("name", "kimchy"))
                .setSize(1);
        MultiSearchResponse multiSearchResponse = client
                .prepareMultiSearch()
                .add(srb1)
                .add(srb2)
                .get();
        //the number of hits
        long msHits = 0;
        for(MultiSearchResponse.Item item : multiSearchResponse.getResponses()){
            SearchResponse response = item.getResponse();
            if (response != null)
                msHits += response.getHits().getTotalHits();
        }
    }

    public void aggregation()throws Exception{
        TransportClient client = NodeClient.getInstance().getClient();
        SearchResponse response = client
                .prepareSearch("persons")
                .setQuery(QueryBuilders.matchAllQuery())
                .addAggregation(AggregationBuilders
                        .terms("agg1")
                        .field("gender"))
                .addAggregation(AggregationBuilders
                        .dateHistogram("agg2")
                        .field("birth")
                        .dateHistogramInterval(DateHistogramInterval.YEAR))
                .get();
        // Get your facet results
        Terms terms = response.getAggregations().get("agg1");
        Histogram histogram = response.getAggregations().get("agg2");
    }

    public void terminateAfter()throws Exception{
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch() //all indices
                .setTerminateAfter(10000)
                .get();
        if (sr.isTerminatedEarly()){
            //we finished the case of early
        }
    }

    public void searchTemplate()throws Exception{
        TransportClient client = NodeClient.getInstance().getClient();
        Map<String, Object> template_params = new HashMap<>();
        template_params.put("param_gender", "male");
        /*SearchResponse sr = new SearchTemplateRequestBuilder(client)
                .setScript("template_gender")
                .setScriptType(ScriptService.ScriptType.FILE)
                .setScriptParams(template_params)
                .setRequest(new SearchRequest())
                .get()
                .getResponse();*/
        //store your template in the cluster state:
        /*client.admin().cluster().preparePutStoredScript()
                .setScriptLang("mustache")
                .setId("template_gender")
                .setSource(new BytesArray(
                        "{\n" +
                                "    \"query\" : {\n" +
                                "        \"match\" : {\n" +
                                "            \"gender\" : \"{{param_gender}}\"\n" +
                                "        }\n" +
                                "    }\n" +
                                "}")).get();
        //To execute a stored templates, use ScriptService.ScriptType.STORED:
        SearchResponse sr = new SearchTemplateRequestBuilder(client)
                .setScript("template_gender")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(template_params)
                .setRequest(new SearchRequest())
                .get()
                .getResponse();*/

        //You can also execute inline templates:
        /*SearchResponse sr = new SearchTemplateRequestBuilder(client)
                .setScript("{\n" +
                        "        \"query\" : {\n" +
                        "            \"match\" : {\n" +
                        "                \"gender\" : \"{{param_gender}}\"\n" +
                        "            }\n" +
                        "        }\n" +
                        "}")
                .setScriptType(ScriptType.INLINE)
                .setScriptParams(template_params)
                .setRequest(new SearchRequest())
                .get()
                .getResponse();*/
        
    }



}
