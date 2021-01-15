package elasticsearchLab;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Bulk {
	private String elasticServer;
	private int port;
	private String protocol;
	private int bulkSize;
	private int currentBulkSize;

	private BulkRequest requests;
	private BulkResponse responses;
	private RestHighLevelClient client;
	private BulkResponseProcessor bulkResponseProcessor;

	public Bulk(String elasticServer, int port, String protocol, int bulkSize) {
		this.elasticServer = elasticServer;
		this.port = port;
		this.protocol = protocol;
		this.bulkSize = bulkSize;
		currentBulkSize = 0;
		this.requests = new BulkRequest();
		bulkResponseProcessor = new BulkResponseProcessor();
	}

	public int feed(String table, Map<String, Object> doc) {
		IndexRequest request = new IndexRequest(table).source(doc);

		requests.add(request);

		setCurrentBulkSize(requests.numberOfActions());

		return getCurrentBulkSize();

	}

	public int getCurrentBulkSize() {
		return currentBulkSize;
	}

	private void setCurrentBulkSize(int currentBulkSize) {
		this.currentBulkSize = currentBulkSize;

		if (currentBulkSize == bulkSize) {
			sendToElastic();
		}
	}

	public void sendToElastic() {
		try {
			client = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticServer, port, protocol)));
			responses = client.bulk(requests, RequestOptions.DEFAULT);
			requests.requests().clear();
			client.close();
			this.currentBulkSize = 0;
			bulkResponseProcessor.work(responses);
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	public Boolean isEmpty() {
		return currentBulkSize == 0 ? true : false;
		
	}

}
