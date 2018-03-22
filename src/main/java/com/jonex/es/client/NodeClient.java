package com.jonex.es.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;

/**
 * Created by xubai on 2018/03/17 上午1:17.
 */
public class NodeClient {

    private TransportClient client;

    private static class NodeClientHolder{
        private static NodeClient instance = new NodeClient();
    }

    public static NodeClient getInstance(){
        return NodeClientHolder.instance;
    }

    public TransportClient getClient(){
        return this.client;
    }

    public void start()throws Exception{
        // 配置信息
        Settings esSetting = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        client = new TransportClient(esSetting, new ArrayList<Class<? extends Plugin>>()) {
            @Override
            public List<TransportAddress> transportAddresses() {
                List<TransportAddress> list = new ArrayList<TransportAddress>();
                list.add(new TransportAddress(new InetSocketAddress("127.0.0.1", 9200)));
                return list;
            }
        };

    }

    public void close(){
        if (client != null)
            // on shutdown
            client.close();
    }

    public static void main(String[] args) {
        NodeClient client = new NodeClient();
        try{
            client.start();
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            client.close();
        }
    }


}
