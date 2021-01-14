package elasticsearchLab;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Main {
	public static void main(String[] args) throws IOException {
		System.out.println("Hello Teste");
		String table = "teste";
		String elasticServer = "localhost";
		int port = 9200;
		String protocol = "http";

		Map<String, Object> doc1 = new HashMap<>();
		feedDoc(doc1, 1);
		sendToBulk(table, doc1, elasticServer, port, protocol);
		
		Map<String, Object> doc2 = new HashMap<>();
		feedDoc(doc2, 2);
		sendToBulk(table, doc2, elasticServer, port, protocol);
		
		Map<String, Object> doc3 = new HashMap<>();
		feedDoc(doc3, 3);		
		sendToBulk(table, doc3, elasticServer, port, protocol);
		

	}

	private static void sendToBulk(String table, Map<String, Object> doc, String elasticServer, int port, String protocol) {
		
		BulkRequest requests = new BulkRequest();
		IndexRequest request1 = new IndexRequest(table).source(doc);

		requests.add(request1);

		BulkResponse responses;
		try {
			RestHighLevelClient client = new RestHighLevelClient(
					RestClient.builder(new HttpHost(elasticServer, port, protocol)));
			responses = client.bulk(requests, RequestOptions.DEFAULT);
			client.close();
			
			if (responses.hasFailures()) {
				System.out.println("erroooooou");
			}			
			new BulkResponseProcessor(responses);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	private static void feedDoc(Map<String, Object> doc1, int num) {
		doc1.put("numero", num);
		doc1.put("postDate", new Date());
	}
}
