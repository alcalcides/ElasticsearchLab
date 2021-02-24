package elasticsearchLab;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;

public class BulkResponseProcessor {

	public void work(BulkResponse responses) {
		if (responses.hasFailures()) {
			for (BulkItemResponse response : responses) {
				if (response.isFailed()) {
					System.out.println(response.getFailure());
				}
			}
		}
	}
}
