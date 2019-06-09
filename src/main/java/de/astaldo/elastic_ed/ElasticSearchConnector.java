package de.astaldo.elastic_ed;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;

import de.astaldo.elastic_ed.elastic_search.LoggingBulkProcessorListener;

public class ElasticSearchConnector {

    private final String host;
    private final int port;
    private final String protocol;
    private final String core;
    
    private RestHighLevelClient client = null;
    private BulkProcessor bulkProcessor = null;
    
    private static final Log logger = LogFactory.getLog(ElasticSearchConnector.class);
    
    public ElasticSearchConnector(String host, String core) {
        this(host, 9200, "http", core);
        createClient();
        addBulkProcessor(this.client);
    }
    
    public ElasticSearchConnector(String host, int port, String protocol, String core) {
        super();
        this.host = host;
        this.port = port;
        this.protocol = protocol;
        this.core = core;
    }
    
    private void createClient() {
        this.client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port, protocol)));
        logger.info("Connected to " + host + ":" + port);
    }
    public void disconnect(Client client) {
        try {
            logger.info("disconnecting");
            if(bulkProcessor != null) {
                bulkProcessor.awaitClose(1, TimeUnit.MINUTES);
            }
            if(client != null) {
                client.close();
            }
        }
        catch (Exception e) {
            logger.fatal("Cannot disconnect.",e);
        }
    }
    public Listener addBulkProcessor(RestHighLevelClient client) {
        return addBulkProcessor(client, 30, 1000);
    }
    public Listener addBulkProcessor(RestHighLevelClient client, int flushIntervalSeconds, int bulkActions) {
        Listener bulkProcessorListener = new LoggingBulkProcessorListener();
        BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) ->
                    client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                bulkProcessorListener);
        
        this.bulkProcessor = builder
                .setConcurrentRequests(1)
                .setFlushInterval(TimeValue.timeValueSeconds(flushIntervalSeconds))
                .setBulkActions(bulkActions)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
        return bulkProcessorListener;
    }
    public void flushPending() {
        if(bulkProcessor != null) {
            bulkProcessor.flush();
        }
    }
    public void addDocument(String documentId, String json) throws IOException {
        bulkProcessor.add(
                new IndexRequest(core)
                    .id(documentId)
                    .source(json, XContentType.JSON));
    }
    public void updateDocument(String documentId, Map<String, Object> data) {
        bulkProcessor.add(
                new UpdateRequest(core, documentId)
                    .doc(data));
    }
    public void removeDocument(String documentId) throws IOException {
        bulkProcessor.add(
                new DeleteRequest(core)
                    .id(documentId));
    }
    
    public RestHighLevelClient getClient() {
        return client;
    }

    private void updateAllBodiesWithSystemId64(Number id64, Script script, ActionListener<BulkByScrollResponse> updateListener) {
        UpdateByQueryRequest u = new UpdateByQueryRequest(EDSMTool.CORE_BODIES)
                .setQuery(new TermQueryBuilder("systemId64", id64))
                .setScript(script);
        client.updateByQueryAsync(u, RequestOptions.DEFAULT, updateListener); 
    }

    public boolean documentExists(String documentId) throws IOException {
        GetResponse response = client.get(new GetRequest(this.core, documentId), RequestOptions.DEFAULT);
        return !response.isExists();
    }

}
