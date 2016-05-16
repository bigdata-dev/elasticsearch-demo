package com.ryxc.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

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
     * 设置 client.transport.sniff为true,使客户端嗅探整个集群状态，把集群中其他机器的IP地址加到客户端
     * 好处：一般不用手动在客户端段设置集群里所有机器的ip，他会自动添加，并且自动发现新加入的集群的机器
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

    TransportClient client = null;

    @Before
    public void before(){
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true)
                .build();

        client = new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress("ryxc181",9300));
        ImmutableList<DiscoveryNode> discoveryNodes = client.connectedNodes();
        for (DiscoveryNode discoveryNode:
                discoveryNodes) {
            System.out.println(discoveryNode.getHostAddress());
        }

    }

    /**
     * index json
     */
    @Test
    public void  test4(){
        String jsonStr = "{\"name\":\"zs\",\"age\":20}";
        IndexResponse indexResponse = client.prepareIndex(index, type, "3").setSource(jsonStr).get();
        System.out.println(indexResponse.getVersion());
    }


    /**
     * index map
     */
    @Test
    public void test5(){
        HashMap<String, Object> hashMap = new HashMap<String, Object>();
        hashMap.put("name","lisi");
        hashMap.put("age",27);
        IndexResponse response = client.prepareIndex(index, type, "3").setSource(hashMap).get();
        System.out.println(response.getVersion());
    }

    /**
     * index bean
     */
    @Test
    public void test6(){
        Person person = new Person();
        person.setName("wangwu");
        person.setAge(8);

        IndexRequestBuilder requestBuilder = client.prepareIndex(index, type, "3").setSource(JSONObject.toJSONString(person));
        IndexResponse response = requestBuilder.get();
        System.out.println(response.getVersion());
    }

    /**
     * index es helper
     */
    @Test
    public void test7() throws IOException{
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "helper")
                .field("score", 60)
                .endObject();

        IndexResponse response = client.prepareIndex(index, type, "3").setSource(builder).get();
        System.out.println(response.getVersion());

    }


    /**
     * 通过ID查询
     */
    @Test
    public void test8(){
//        GetResponse response = client.prepareGet(index, type, "3").get();
        GetResponse response = client.prepareGet(index, type, "3").execute().actionGet();
        System.out.println(response.getSourceAsString());
    }

    /**
     * 局部更新1
     * @throws IOException
     */
    @Test
    public void test9() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("city", "shanghai").endObject();
        UpdateResponse updateResponse = client.prepareUpdate(index, type, "3").setDoc(builder).get();
        System.out.println(updateResponse.getVersion());
    }

    /**
     * 局部更新2
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void test10() throws IOException, ExecutionException, InterruptedException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().field("fac", "3334").endObject();
        UpdateRequest updateRequest = new UpdateRequest(index, type, "3").doc(xContentBuilder);
        UpdateResponse response = client.update(updateRequest).get();
        System.out.println(response.getVersion());
    }

    /**
     * 插入或者更新
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void test11() throws IOException, ExecutionException, InterruptedException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("name", "修改").endObject();
        XContentBuilder builder1 = XContentFactory.jsonBuilder().startObject().field("name", "xxxx").field("age", 777).endObject();

        UpdateRequest updateRequest = new UpdateRequest(index, type, "4");
        updateRequest.doc(builder);
        updateRequest.upsert(builder1);

        UpdateResponse response = client.update(updateRequest).get();
        System.out.println(response.getVersion());

    }

    /**
     * 删除
     */
    @Test
    public void test12(){
        DeleteResponse response = client.prepareDelete(index, type, "4").get();
        System.out.println(response.getVersion());
    }

    /**
     * count操作
     */
    @Test
    public void test13(){
        CountResponse response = client.prepareCount(index).get();
        System.out.println(response.getCount());
    }


    /**
     * bulk批量操作
     * @throws IOException
     */
    @Test
    public void test14() throws IOException {
        BulkRequestBuilder prepareBulk = client.prepareBulk();

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("name", "ssddd").field("age", 9999).endObject();
        IndexRequest indexRequest = new IndexRequest(index,type);
        indexRequest.id("8");
        indexRequest.source(builder);

        DeleteRequest deleteRequest = new DeleteRequest(index, type, "3");

        prepareBulk.add(indexRequest);
        prepareBulk.add(deleteRequest);

        BulkResponse responses = prepareBulk.get();

        if(responses.hasFailures()){
            BulkItemResponse[] items = responses.getItems();
            for (BulkItemResponse bulkItemResponse:
                 items) {
                System.out.println(bulkItemResponse.getFailureMessage());
            }
        }

    }




}
