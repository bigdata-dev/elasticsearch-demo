package com.ryxc.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        hashMap.put("name","ww");
        hashMap.put("age",17);
        IndexResponse response = client.prepareIndex(index, type).setSource(hashMap).get();
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
                .field("name", "li5")
                .field("score", 50)
                .endObject();

        IndexResponse response = client.prepareIndex(index, type).setSource(builder).get();
        //IndexResponse response = client.prepareIndex(index, type, "3").setSource(builder).get();
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


    /**
     * 	 * 查询
     *
     *
     *
     * filter过滤
     * 后面使用from to的时候，默认都是闭区间。都包含。
     * gt：大于
     * gte：大于等于
     * lt：小于
     * lte：小于等于
     */
    @Test
    public void test15(){
        SearchResponse searchResponse = client.prepareSearch(index) //指定索引库
                .setTypes(type) //指定查询索引库下的类型
                .setSearchType(SearchType.QUERY_THEN_FETCH) //指定查询的方式
                .addSort("age",SortOrder.ASC)
                //.setQuery(QueryBuilders.matchQuery("name", "zs")) //指定查询条件

                //.setPostFilter(FilterBuilders.rangeFilter("age").gt(10).lte(19))
                //.setPostFilter(FilterBuilders.rangeFilter("age").from(10).to(19))

                .setFrom(0)
                .setSize(10)
                .setExplain(true)  //针对查询的数据按照相关度排序
                .get();

        SearchHits searchHits = searchResponse.getHits();
        long totalHits = searchHits.getTotalHits();
        System.out.println("totalHits:"+totalHits);

        SearchHit[] hits = searchHits.getHits();

        for (SearchHit hit:
             hits) {
            System.out.println(hit.getSourceAsString());

        }

    }


    /**
     *  高亮
     * @throws Exception
     */
    @Test
    public void test16() throws Exception {

        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("name", "zs"))
                //设置高亮
                .addHighlightedField("name")
                .setHighlighterPreTags("<font color='red'>")
                .setHighlighterPostTags("</font>")
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .get();

        SearchHits searchHits = searchResponse.getHits();
        System.out.println("totalHits:"+searchHits.getTotalHits());

        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit:hits) {
            //获取高亮内容
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //查询高亮字段内容
            HighlightField  highlightField = highlightFields.get("name");
            Text[] fragments = highlightField.getFragments();
            for (Text text:fragments
                 ) {
                System.out.println(text);
            }

            System.out.println(hit.getSourceAsString());
        }

    }

    /**
     * 删除索引库
     * @throws Exception
     */
    @Test
    public void test17() throws Exception {
        client.admin().indices().prepareDelete(index).get();

    }

    /**
     * 统计每个年龄有多少人
     * @throws Exception
     */
    @Test
    public void test18() throws Exception {

        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addAggregation(AggregationBuilders.terms("age_terms").field("age"))
                .get();

        Terms terms = searchResponse.getAggregations().get("age_terms");
        List<Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket:buckets
             ) {
            System.out.println(bucket.getKey()+"---"+bucket.getDocCount());
        }
    }

    /**
     * 对name进行分组 score 求和
     * 默认情况下，只能返回前10组分组的数据,需要使用size方法，设置为0，这样就会把所有分组数据都返回
     * @throws Exception
     */
    @Test
    public void test19() throws Exception {
        SearchResponse searchResponse = client.prepareSearch(index)
                .setTypes(type)
                .addAggregation(AggregationBuilders.terms("name_terms").field("name").size(0)
                        .subAggregation(AggregationBuilders.sum("score_sum").field("score")))
                .get();

        Terms terms = searchResponse.getAggregations().get("name_terms");

        List<Bucket> buckets = terms.getBuckets();
        for (Bucket bucket:buckets
             ) {
            Sum sum = bucket.getAggregations().get("score_sum");
            System.out.println(bucket.getKey()+"----"+ sum.getValue());
        }


    }

    /**
     * 使用不同的分片查询方式查询数据
     * @throws Exception
     */
    @Test
    public void test20() throws Exception {
        //优先在本地节点有的分片中查询，没有的话在到其他节点查询
        //SearchResponse searchResponse = client.prepareSearch(index).setPreference("_local")
        //只在主机分片中查询
        //SearchResponse searchResponse = client.prepareSearch(index).setPreference("_primary")
        //先在主机分片中查询，主分片找不到(挂了)，就在副本中查询
        //SearchResponse searchResponse = client.prepareSearch(index).setPreference("_primary_first")
        //只在节点ID下查询
        //SearchResponse searchResponse = client.prepareSearch(index).setPreference("_only_node:A_LtqRAHQ8GIO-1ttTSukQ")
        //优先在指定节点ID下查询
        //SearchResponse searchResponse = client.prepareSearch(index).setPreference("_prefer_node:A_LtqRAHQ8GIO-1ttTSukQ")
        //查询指定分片的数据
          SearchResponse searchResponse = client.prepareSearch(index).setPreference("_shards:0")


                .setTypes(type)
                .addSort("score",SortOrder.ASC)
                .setFrom(0)
                .setSize(10)
                .setExplain(true)
                .get();

        SearchHits searchHits = searchResponse.getHits();
        System.out.println("总数："+searchHits.getTotalHits());

        SearchHit[] hits = searchHits.getHits();
        for (SearchHit searchHit:hits
             ) {
            System.out.println(searchHit.getSourceAsString());
        }

    }
}
