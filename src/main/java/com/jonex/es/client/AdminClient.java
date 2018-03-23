package com.jonex.es.client;

import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;

/**
 * <pre>
 *
 *  File: AdminClient.java
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
public class AdminClient {


    public static org.elasticsearch.client.AdminClient getAdminClient(){
        return NodeClient.getInstance().getClient().admin();
    }

    public static IndicesAdminClient getIndicesAdminClient(){
        return NodeClient.getInstance().getClient().admin().indices();
    }

    public static ClusterAdminClient getClusterAdminClient(){
        return NodeClient.getInstance().getClient().admin().cluster();
    }

}
