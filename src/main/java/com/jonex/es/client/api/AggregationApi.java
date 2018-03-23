package com.jonex.es.client.api;

import com.jonex.es.client.NodeClient;
import jdk.nashorn.internal.objects.Global;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsAggregationBuilder;

/**
 * <pre>
 *
 *  File: AggregationApi.java
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
public class AggregationApi {


    public void aggregation()throws Exception{
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch()
                .setQuery(QueryBuilders.termQuery("gender", "male"))
                .addAggregation(AggregationBuilders.terms("age"))
                .execute()
                .actionGet();
    }

    public void subAggregation()throws Exception{
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch()
                .addAggregation(
                        AggregationBuilders.terms("by_country").field("country")
                                .subAggregation(AggregationBuilders.dateHistogram("by_year")
                                        .field("dateOfBirth")
                                        .dateHistogramInterval(DateHistogramInterval.YEAR)
                                        .subAggregation(AggregationBuilders.avg("avg_children").field("children"))
                                )
                )
                .execute().actionGet();
    }

    public void metricsAggregation()throws Exception{
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .addAggregation(AggregationBuilders
                        .min("agg")
                        .field("height"))
                .execute()
                .actionGet();
       /* MinAggregationBuilder aggregation =AggregationBuilders
                        .min("agg")
                        .field("height");
        // sr is here your SearchResponse object
        Min agg = sr.getAggregations().get("agg");
        double min = agg.getValue();*/

        /*MaxAggregationBuilder aggregation = AggregationBuilders
                        .max("agg")
                        .field("height");
        // sr is here your SearchResponse object
        Max agg = sr.getAggregations().get("agg");
        double max = agg.getValue();*/

        /*// stats
        StatsAggregationBuilder aggregation =
                AggregationBuilders
                        .stats("agg")
                        .field("height");
        // sr is here your SearchResponse object
        Stats statsAgg = sr.getAggregations().get("agg");
        double min = statsAgg.getMin();
        double max = statsAgg.getMax();
        double avg = statsAgg.getAvg();
        double sum = statsAgg.getSum();
        long count = statsAgg.getCount();*/

        //avg
        //count
        //percentiles
        //percentiles
        //cardinality
        //geoBounds
        //topHits
        //scriptedMetric
        //
    }

    public void bucketAggregation()throws Exception{
        /*//global aggregation
        SearchResponse sr = NodeClient.getInstance().getClient()
                .prepareSearch("persons")
                .addAggregation(AggregationBuilders
                        .global("agg")
                        .subAggregation(AggregationBuilders.terms("genders").field("gender")))
                .execute()
                .actionGet();
        // sr is here your SearchResponse object
        Global agg = sr.getAggregations().get("agg");
        agg.getDocCount(); // Doc count*/

        /*//filter aggregation
        AggregationBuilders
                .filter("agg", QueryBuilders.termQuery("gender", "male"));
        // sr is here your SearchResponse object
        Filter agg = sr.getAggregations().get("agg");
        agg.getDocCount(); // Doc count*/

        /*//filters aggregation
        AggregationBuilder aggregation =AggregationBuilders
                .filters("agg",
                        new FiltersAggregator.KeyedFilter("men", QueryBuilders.termQuery("gender", "male")),
                        new FiltersAggregator.KeyedFilter("women", QueryBuilders.termQuery("gender", "female")));
        // sr is here your SearchResponse object
        Filters agg = sr.getAggregations().get("agg");
        // For each entry
        for (Filters.Bucket entry : agg.getBuckets()) {
            String key = entry.getKeyAsString();            // bucket key
            long docCount = entry.getDocCount();            // Doc count
            logger.info("key [{}], doc_count [{}]", key, docCount);
        }*/

        // missing
        //nested
        //reverse
        //children
        //terms
        //significant terms
        //range
        //date range
        //ip range
        //histogram
        //date histogram
        //geo distance
        //geo hash grid
    }



}
