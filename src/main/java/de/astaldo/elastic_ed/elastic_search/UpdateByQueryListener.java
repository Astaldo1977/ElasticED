package de.astaldo.elastic_ed.elastic_search;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.reindex.BulkByScrollResponse;

public class UpdateByQueryListener implements ActionListener<BulkByScrollResponse> {

    private static final Log logger = LogFactory.getLog(UpdateByQueryListener.class);
    
    @Override
    public void onResponse(BulkByScrollResponse response) {
//        if(response.getUpdated() > 0) {
//            logger.info("Updated " + response.getUpdated() + " bodies");
//        }
        
    }

    @Override
    public void onFailure(Exception e) {
        logger.fatal("Failure on Update by Query",e);
    }
}
