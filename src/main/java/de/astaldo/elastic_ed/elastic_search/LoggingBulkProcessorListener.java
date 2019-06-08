package de.astaldo.elastic_ed.elastic_search;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;

public class LoggingBulkProcessorListener implements Listener{
    private static final Log logger = LogFactory.getLog(LoggingBulkProcessorListener.class);
    
    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
        logger.debug(request.estimatedSizeInBytes() + " bytes " + response.status());
        if (response.hasFailures()) {
            logger.warn(response.buildFailureMessage());
        }
    }
    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        logger.warn(failure.getMessage());
    }

}
