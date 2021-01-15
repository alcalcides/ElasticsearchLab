package elasticsearchLab;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;

public class BulkResponseProcessor {	
	
	public void work(BulkResponse responses) {
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
	}
}
