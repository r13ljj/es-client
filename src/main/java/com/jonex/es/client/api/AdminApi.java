package com.jonex.es.client.api;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.jonex.es.client.AdminClient;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.PutStoredScriptResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * <pre>
 *
 *  File: AdminApi.java
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
public class AdminApi {

    /**=========================================
     * index api
     */
    public void createIndex(){
        //with default settings
        CreateIndexResponse createIndexResponse = AdminClient.getIndicesAdminClient().prepareCreate("twitter").get();
        //special settings
        AdminClient.getIndicesAdminClient().prepareCreate("twitter")
                .setSettings(Settings.builder()
                        .put("index.number_of_shards", 3)
                        .put("index.number_of_replicas", 2)
                )
                .get();
    }

    public void putMapping(){
        AdminClient.getIndicesAdminClient().prepareCreate("twitter")
                .addMapping("\"tweet\": {\n" +
                        "  \"properties\": {\n" +
                        "    \"message\": {\n" +
                        "      \"type\": \"text\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}")
                .get();
        //special the type
        AdminClient.getIndicesAdminClient().preparePutMapping("twitter")
                .setType("user")
                .setSource("{\n" +
                        "  \"properties\": {\n" +
                        "    \"name\": {\n" +
                        "      \"type\": \"text\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}", XContentType.JSON)
                .get();
        // You can also provide the type in the source document
        AdminClient.getIndicesAdminClient().preparePutMapping("twitter")
                .setType("user")
                .setSource("{\n" +
                        "    \"user\":{\n" +
                        "        \"properties\": {\n" +
                        "            \"name\": {\n" +
                        "                \"type\": \"text\"\n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}", XContentType.JSON)
                .get();
    }

    public void refreshIndex(){
        RefreshResponse refreshResponse = AdminClient.getIndicesAdminClient().prepareRefresh("twitter").get();
        AdminClient.getIndicesAdminClient().prepareRefresh().get();
        AdminClient.getIndicesAdminClient().prepareRefresh("twitter", "type").get();

    }

    public void getIndexSettings(){
        GetSettingsResponse response = AdminClient.getIndicesAdminClient().prepareGetSettings("company", "employee").get();
        response.getIndexToSettings().forEach (cursor -> {
            String index = cursor.key;
            Settings settings = cursor.value;
            Integer shards = settings.getAsInt("index.number_of_shards", null);
            Integer replicas = settings.getAsInt("index.number_of_replicas", null);
        });
    }

    public void updateIndexSettings(){
        AdminClient.getIndicesAdminClient().prepareUpdateSettings("twitter")
                .setSettings(Settings.builder()
                            .put("index.number_of_replicas", 3)
                )
                .get();
    }

    /**=======================================
     * cluster index
     */
    public void clusterHealth(){
        ClusterHealthResponse healths = AdminClient.getClusterAdminClient().prepareHealth().get();
        String clusterName = healths.getClusterName();
        int numberOfDataNodes = healths.getNumberOfDataNodes();
        int numberOfNodes = healths.getNumberOfNodes();
        for (ClusterIndexHealth health : healths.getIndices().values()) {
            String index = health.getIndex();
            int numberOfShards = health.getNumberOfShards();
            int numberOfReplicas = health.getNumberOfReplicas();
            ClusterHealthStatus status = health.getStatus();
        }
    }

    public void setClusterStatus(){
        AdminClient.getClusterAdminClient().prepareHealth()
                .setWaitForYellowStatus()
                .get();
        AdminClient.getClusterAdminClient().prepareHealth("company")
                .setWaitForGreenStatus()
                .get();

        AdminClient.getClusterAdminClient().prepareHealth("employee")
                .setWaitForGreenStatus()
                .setTimeout(TimeValue.timeValueSeconds(2))
                .get();

        ClusterHealthResponse response = AdminClient.getClusterAdminClient().prepareHealth().get();
        ClusterHealthStatus status = response.getIndices().get("company").getStatus();
        if (!status.equals(ClusterHealthStatus.GREEN)) {
            throw new RuntimeException("Index is in " + status + " state");
        }
    }

    /**======================================
     * storedScript api
     */
    public void putStoreScript(){
        PutStoredScriptResponse response = AdminClient.getClusterAdminClient().preparePutStoredScript()
                .setId("script1")
                .setContent(new BytesArray("{\"script\": {\"lang\": \"painless\", \"source\": \"_score * doc['my_numeric_field'].value\"} }"), XContentType.JSON)
                .get();
    }
    public void getStoredScript(){
        GetStoredScriptResponse response = AdminClient.getClusterAdminClient().prepareGetStoredScript()
                .setId("script1")
                .get();
    }
    public void deleteStoredScript(){
        DeleteStoredScriptResponse response = AdminClient.getClusterAdminClient().prepareDeleteStoredScript()
                .setId("script1")
                .get();
    }







}
