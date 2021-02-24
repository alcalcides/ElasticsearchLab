package elasticsearchLab;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Main {
	public static void main(String[] args) throws IOException {
		System.out.println("play");

		String table = "teste";
		String elasticServer = "localhost";
		int port = 9200;
		int bulkSize = 50;
		String protocol = "http";
		Bulk bulk = new Bulk(elasticServer, port, protocol, bulkSize);

		for (int i = 0; i < 112; i++) {
			Map<String, Object> doc = new HashMap<>();
			feedDoc(doc, i);
			bulk.feed(table, doc);
		}
		Map<String, Object> doc = new HashMap<>();
		doc.put("numero", "recife");
		bulk.feed(table, doc);

		if (!bulk.isEmpty()) {
			bulk.sendToElastic();
		}

		System.out.println("ok");
	}

	private static void feedDoc(Map<String, Object> doc1, int num) {
		doc1.put("numero", num);
		doc1.put("postDate", new Date());
	}
}
