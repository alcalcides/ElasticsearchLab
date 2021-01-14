package elasticsearchLab;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Main {
	public static void main(String[] args) throws IOException {
		System.out.println("Hello Teste");
		
		RestHighLevelClient client = new RestHighLevelClient(
		        RestClient.builder(
		                new HttpHost("localhost", 9200, "http")));
		
		Map<String, Object> doc = new HashMap<>();
		
		doc.put("user", "Alcides");
		doc.put("postDate", new Date());
		doc.put("message", "trying out Elasticsearch");
		
		IndexRequest request = new IndexRequest("posts")
		    .id("1").source(doc); 

		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
		
		System.out.println(indexResponse);
		
		client.close();

	}
}
