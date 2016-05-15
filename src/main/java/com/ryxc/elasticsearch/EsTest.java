package com.ryxc.elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.Callable;

public class EsTest {
    private String index = "ryxc";
    private String type = "emp";

    /**
     * 默认情况下是可以使用的，如果集群的名称不是默认的elasticsearch就不能使用
     */
    @Test
    public void test1() {

        TransportClient transportClient = new TransportClient();
        transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc181", 9300));
        transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc182", 9300));
        transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc183", 9300));
        GetResponse getResponse = transportClient.prepareGet(index, type, "21").get();
        System.out.println(getResponse.getSourceAsString());
    }

    /**
     * 对集群的默认名称修改，代码需要这样写
     */
    @Test
    public void test2() {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient transportClient = new TransportClient(settings);
        transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc181", 9300));
        transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc182", 9300));
        transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc183", 9300));

        GetResponse getResponse = transportClient.prepareGet(index, type, "21").get();
        System.out.println(getResponse.getSourceAsString());

    }

    /**
     * 设置 client.transport.sniff为true,使客户端嗅探整个集群状态，把集群中其他集群的IP地址加到客户端
     * 好处：一般不用手动在客户端段设置集群里所有集群的ip，他会自动添加，并且自动发现新加入的集群的机器
     */
    @Test
    public void test3() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true)
                .build();

        TransportClient transportClient = new TransportClient(settings);
        transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc181",9300));
        ImmutableList<DiscoveryNode> discoveryNodes = transportClient.connectedNodes();
        for (DiscoveryNode discoveryNode:
             discoveryNodes) {
            System.out.println(discoveryNode.getHostAddress());
        }

        GetResponse getResponse = transportClient.prepareGet(index, type, "21").get();
        System.out.println(getResponse.getSourceAsString());
    }



}
