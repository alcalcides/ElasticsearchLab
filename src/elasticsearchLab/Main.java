package elasticsearchLab;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Main {
	public static void main(String[] args) throws IOException {
		System.out.println("Hello Teste");
		String table = "teste";

		String elasticServer = "localhost";
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost(elasticServer, 9200, "http")));
		
		BulkRequest requests = new BulkRequest();

		Map<String, Object> doc1 = new HashMap<>();
		Map<String, Object> doc2 = new HashMap<>();
		Map<String, Object> doc3 = new HashMap<>();

		feedDoc(doc1, 1);
		feedDoc(doc2, 2);
		feedDoc(doc3, 3);

		IndexRequest request1 = new IndexRequest(table).id("1").source(doc1);
		IndexRequest request2 = new IndexRequest(table).id("2").source(doc2);
		IndexRequest request3 = new IndexRequest(table).id("3").source(doc3);

		requests.add(request1);
		requests.add(request2);
		requests.add(request3);

		BulkResponse responses = client.bulk(requests, RequestOptions.DEFAULT);

		if (responses.hasFailures()) {
			System.out.println("erroooooou");
		}

		for (BulkItemResponse response : responses) {
			DocWriteResponse itemResponse = response.getResponse();

			switch (response.getOpType()) {
			case INDEX:
				IndexResponse indexResponse = (IndexResponse) itemResponse;
				System.out.println("index => " + indexResponse);
				break;
			case CREATE:
				IndexResponse createResponse = (IndexResponse) itemResponse;
				System.out.println("create => " + createResponse);
				break;
			case UPDATE:
				UpdateResponse updateResponse = (UpdateResponse) itemResponse;
				System.out.println("update => " + updateResponse);
				break;
			case DELETE:
				DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
				System.out.println("delete => " + deleteResponse);
			}
		}

		client.close();

	}

	private static void feedDoc(Map<String, Object> doc1, int num) {
		doc1.put("numero", num);
		doc1.put("postDate", new Date());
	}
}
