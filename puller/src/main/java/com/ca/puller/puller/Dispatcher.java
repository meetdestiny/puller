package com.ca.puller.puller;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.util.List;
import java.util.Map;

public class Dispatcher {	
	JestClient client;
	Integer processed ; 


	public Dispatcher() {
		try {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://ca-audit-elasticsearch.ca-stg-analytics.ca-audit.services.prod.walmart.com:9200")
		//.multiThreaded(true)
		.connTimeout(15000).readTimeout(30000)
		.build());
		client = factory.getObject();
		}catch(Exception ex) {
			ex.printStackTrace();
		}

	}

	public void dispatch(Map map,String indexName) {
		Index index = new Index.Builder(map).index(indexName ).type("accesslog").build();
		try {
			client.execute(index);
		} catch (Exception e) {
			System.err.println("Could not dispatch ");
			e.printStackTrace();
		}
	}

	public void dispatchAll(List<Map> logs,String indexName) {
		System.out.println("Dispatching " + logs.size() + " Log Records");
		for( Map map: logs) {
			if( map.size() ==0) 
				continue;

			Index index = new Index.Builder(map).index(indexName ).type("accesslog").build();
			try {
				client.execute(index);
				
			} catch (Exception e) {
				System.out.println("Could not dispatch");
				e.printStackTrace();
			}
		}
	}

}
