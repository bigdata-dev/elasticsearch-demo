package com.ryxc.elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Test;

public class EsTest {
	private String index = "ryxc";
	private String type = "emp";
	
	/**
	 * 默认情况下是可以使用的，如果集群的名称不是默认的elasticsearch就不能使用
	 */
	@Test
	public void test1(){
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
	public void test2(){                                            
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
		TransportClient transportClient = new TransportClient(settings);		
		transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc181",9300));
		transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc182",9300));
		transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc183",9300));
		
		GetResponse getResponse = transportClient.prepareGet(index,type,"21").get();
		System.out.println(getResponse.getSourceAsString());
		
	}

	@Test
	public void test3(){
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
		TransportClient transportClient = new TransportClient(settings);
		transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc181",9300));
		transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc182",9300));
		transportClient.addTransportAddress(new InetSocketTransportAddress("ryxc183",9300));

		GetResponse getResponse = transportClient.prepareGet(index,type,"21").get();
		System.out.println(getResponse.getSourceAsString());

	}
	
}
