package com.jonex.es.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * Created by xubai on 2018/03/20 上午1:28.
 */
public class IndexApi {

    public void prepareIndex()throws Exception{
        /*IndexResponse response = NodeClient.getInstance().getClient().prepareIndex("twitter", "tweet", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
                .get();*/
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        IndexResponse response = NodeClient.getInstance().getClient().prepareIndex("twitter", "tweet")
                .setSource(json, XContentType.JSON)
                .get();
        System.out.println(response.getResult());
        // Index name
        String _index = response.getIndex();
        // Type name
        String _type = response.getType();
        // Document ID (generated or not)
        String _id = response.getId();
        // Version (if it's the first time you index this document, you will get: 1)
        long _version = response.getVersion();
        // status has stored current instance statement.
        RestStatus status = response.status();
    }

    public void prepareGet(){
        GetResponse response = NodeClient.getInstance().getClient().prepareGet("twitter", "tweet", "1").get();
    }

    public void prepareDel(){
        DeleteResponse response = NodeClient.getInstance().getClient().prepareDelete("twitter", "tweet", "1").get();
    }

    public void deleteByQuery()throws Exception{
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient())
                .filter(QueryBuilders.matchQuery("gender", "male"))//query
                .source("persons")//index
                .get();//execute the operation
        long deleted = response.getDeleted();

        DeleteByQueryAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient())
                .filter(QueryBuilders.matchQuery("gender", "male"))//query
                .source("persons")//index
                .execute(new ActionListener<BulkByScrollResponse>() {
                    public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                        long deleted = bulkByScrollResponse.getDeleted();
                    }

                    public void onFailure(Exception e) {
                        //handle exception
                    }
                });
    }

    public void update()throws Exception{
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("persons");
        updateRequest.type("type");
        updateRequest.id("1");
        updateRequest.doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject());
        UpdateResponse response = NodeClient.getInstance().getClient().update(updateRequest).get();
    }

    public void prepareUpdate()throws Exception{
        NodeClient.getInstance().getClient().prepareUpdate("twitter", "tweet", "1")
                .setDoc(jsonBuilder()
                    .startObject()
                    .field("gender", "male")
                    .endObject())
                .get();

        NodeClient.getInstance().getClient().prepareUpdate("twitter", "tweet", "1")
                .setScript(new Script(ScriptType.INLINE, "groovy", "ctx._source.gender=\"male\"", null))
                .get();

    }

    public void upsert()throws Exception{
        IndexRequest indexRequest = new IndexRequest("index", "type", "1")
                .source(jsonBuilder()
                        .startObject()
                        .field("name", "Joe Smith")
                        .field("gender", "male")
                        .endObject());
        UpdateRequest updateRequest = new UpdateRequest("index", "type", "1")
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject())
                .upsert(indexRequest);
        NodeClient.getInstance().getClient().update(updateRequest).get();
    }

    public void multiGet()throws Exception{
        MultiGetResponse responses = NodeClient.getInstance().getClient()
                .prepareMultiGet()
                .add("twitter", "tweet", "1")
                .add("twitter", "tweet", "12", "13")
                .add("persons", "type", "foo")
                .get();
        for(MultiGetItemResponse response : responses){
            GetResponse getResponse = response.getResponse();
            if (getResponse.isExists()){
                String json = getResponse.getSourceAsString();
            }
        }

    }

    public void bulkRequest()throws Exception{
        BulkRequestBuilder bulkRequestBuilder =  NodeClient.getInstance().getClient().prepareBulk();
        bulkRequestBuilder.add(NodeClient.getInstance().getClient()
                .prepareIndex("twitter", "tweet", "1")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
        );
        bulkRequestBuilder.add(NodeClient.getInstance().getClient()
                .prepareIndex("twitter", "tweet", "2")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy")
                        .field("postDate", new Date())
                        .field("message", "another post")
                        .endObject()
                )
        );
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            // process failures by iterating through each bulk response item
        }

    }

    public void bulkProcessor()throws Exception{
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                NodeClient.getInstance().getClient(),
                new BulkProcessor.Listener() {
                    public void beforeBulk(long l, BulkRequest bulkRequest) {

                    }

                    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

                    }

                    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

                    }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        bulkProcessor.add(new IndexRequest("twitter", "tweet", "1")
                                .source(jsonBuilder()
                                        .startObject()
                                        .field("", "")
                                        .endObject()));/* your doc here */
        // Add your requests
        bulkProcessor.add(new DeleteRequest("twitter", "tweet", "2"));
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        //bulkProcessor.close();

        // Flush any remaining requests
        bulkProcessor.flush();
        // Or close the bulkProcessor if you don't need it anymore
        bulkProcessor.close();
        // Refresh your indices
        NodeClient.getInstance().getClient().admin().indices().prepareRefresh().get();
        // Now you can start searching!
        NodeClient.getInstance().getClient().prepareSearch().get();

    }

    public void updateByQuery()throws Exception{
        //1 updates each document in an index without changing the source.
        UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient());
        updateByQuery.source("source_index")
                .abortOnVersionConflict(false);
        BulkByScrollResponse response = updateByQuery.get();

        //2 filtering the updated documents, limiting the total number of documents to update, and updating documents with a script:
        UpdateByQueryRequestBuilder updateByQuery2 = UpdateByQueryAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient());
        updateByQuery.source("source_index")
                .filter(QueryBuilders.termQuery("level", "awesome"))
                .size(1000)
                .script(new Script(ScriptType.INLINE, "painless", "ctx._source.awesome = 'absolutely'", null));
        BulkByScrollResponse response2 = updateByQuery.get();

        //3  change the default scroll size or otherwise modify the request for matching documents.
        UpdateByQueryRequestBuilder updateByQuery3 = UpdateByQueryAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient());
        updateByQuery.source("source_index")
                .source()
                .setSize(500);
        BulkByScrollResponse response3 = updateByQuery.get();

        //4 combine size with sorting to limit the documents updated:
        UpdateByQueryRequestBuilder updateByQuery4 = UpdateByQueryAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient());
        updateByQuery.source("source_index").size(100)
                .source().addSort("cat", SortOrder.DESC);
        BulkByScrollResponse response4 = updateByQuery.get();

        //5 In addition to changing the _source field for the document, you can use a script to change the action, similar to the Update API:
        UpdateByQueryRequestBuilder updateByQuery5 = UpdateByQueryAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient());
        updateByQuery.source("source_index")
                .script(new Script(
                        ScriptType.INLINE,
                        "painless",
                        "if (ctx._source.awesome == 'absolutely) {"
                                + "  ctx.op='noop'"
                                + "} else if (ctx._source.awesome == 'lame') {"
                                + "  ctx.op='delete'"
                                + "} else {"
                                + "ctx._source.awesome = 'absolutely'}",
                        null));
        BulkByScrollResponse response5 = updateByQuery.get();

        //6 perform these operations on multiple indices and types at once, similar to the search API:
        UpdateByQueryRequestBuilder updateByQuery6 = UpdateByQueryAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient());
        updateByQuery.source("foo", "bar").source().setTypes("a", "b");
        BulkByScrollResponse response6 = updateByQuery.get();

        //7 If you provide a routing value then the process copies the routing value to the scroll query, limiting the process to the shards that match that routing value:
        UpdateByQueryRequestBuilder updateByQuery7 = UpdateByQueryAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient());
        updateByQuery.source().setRouting("cat");
        BulkByScrollResponse response7 = updateByQuery.get();

        //8 updateByQuery can also use the ingest node by specifying a pipeline like this:

        UpdateByQueryRequestBuilder updateByQuery8 = UpdateByQueryAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient());
        updateByQuery.setPipeline("hurray");
        BulkByScrollResponse response8 = updateByQuery.get();

        //9 fetch the status of all running update-by-query requests with the Task API:
        ListTasksResponse tasksList = NodeClient.getInstance().getClient().admin().cluster().prepareListTasks()
                .setActions(UpdateByQueryAction.NAME).setDetailed(true).get();
        for (TaskInfo info: tasksList.getTasks()) {
            TaskId taskId = info.getTaskId();
            BulkByScrollTask.Status status = (BulkByScrollTask.Status) info.getStatus();
            // do stuff
            //10 With the TaskId shown above you can look up the task directly:
            GetTaskResponse get = NodeClient.getInstance().getClient().admin().cluster().prepareGetTask(taskId).get();

            //11 Any Update By Query can be canceled using the Task Cancel API:
            // Cancel all update-by-query requests
            NodeClient.getInstance().getClient().admin().cluster().prepareCancelTasks().setActions(UpdateByQueryAction.NAME).get().getTasks();
            // Cancel a specific update-by-query request
            NodeClient.getInstance().getClient().admin().cluster().prepareCancelTasks().setTaskId(taskId).get().getTasks();


            //12 Use the _rethrottle API to change the value of requests_per_second on a running update:

            /*RethrottleAction.INSTANCE.newRequestBuilder(NodeClient.getInstance().getClient())
                    .setTaskId(taskId)
                    .setRequestsPerSecond(2.0f)
                    .get();*/
        }


    }

    public void reIndex()throws Exception{
        BulkByScrollResponse response = ReindexAction.INSTANCE
                .newRequestBuilder(NodeClient.getInstance().getClient())
                .destination("target_index")
                .filter(QueryBuilders.matchQuery("category", "xzy"))
                .get();
    }


}
