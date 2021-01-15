package elasticsearchLab;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Main {
	public static void main(String[] args) throws IOException {
		System.out.println("Hello Teste");
		String table = "teste";
		String elasticServer = "localhost";
		int port = 9200;
		int bulkSize = 2;
		String protocol = "http";
		Bulk bulk = new Bulk(elasticServer,port, protocol, bulkSize);

		Map<String, Object> doc1 = new HashMap<>();
		feedDoc(doc1, 1);
		bulk.feed(table, doc1);

		
		Map<String, Object> doc2 = new HashMap<>();
		feedDoc(doc2, 2);
		bulk.feed(table, doc2);

		
		Map<String, Object> doc3 = new HashMap<>();
		feedDoc(doc3, 3);
		bulk.feed(table, doc3);

		
		Map<String, Object> doc4 = new HashMap<>();
		feedDoc(doc4, 4);
		bulk.feed(table, doc4);

		

	}


	private static void feedDoc(Map<String, Object> doc1, int num) {
		doc1.put("numero", num);
		doc1.put("postDate", new Date());
	}
}
