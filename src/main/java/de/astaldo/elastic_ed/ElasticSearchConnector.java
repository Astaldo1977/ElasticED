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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;

import de.astaldo.elastic_ed.elastic_search.LoggingBulkProcessorListener;
import de.astaldo.elastic_ed.elastic_search.UpdateByQueryListener;

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
        return addBulkProcessor(client, 60, 10000);
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
    public void addDocument(String json, String documentId) throws IOException {
        bulkProcessor.add(
                new IndexRequest(core)
                    .id(documentId)
                    .source(json, XContentType.JSON));
    }
    public void removeDocument(String documentId) throws IOException {
        bulkProcessor.add(
                new DeleteRequest(core)
                    .id(documentId));
    }    
    public void scrollAll(SearchRequest searchRequest, ActionListener<SearchResponse> scrollListener) throws IOException {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        if(searchResponse.status() == RestStatus.OK && searchResponse.getScrollId() != null) {
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            ActionListener<BulkByScrollResponse> updateListener = new UpdateByQueryListener();
            while (searchHits != null && searchHits.length > 0) {
                for (int i = 0; i < searchHits.length; i++) {
                    Number id64 = (Number)searchHits[i].getSourceAsMap().get("id64");
                    String elasticSearchId =  searchHits[i].getId();
                    Map<String, Object> coords = (Map<String, Object>) searchHits[i].getSourceAsMap().get("coords");
                    Script script = new Script(
                            ScriptType.INLINE, "painless",
                            "ctx._source.systemCoords=[params.x,params.y,params.z]",
                            coords); 
//                    logger.info("Updating bodies with systemId64: " + id64);
                    updateAllBodiesWithSystemId64(id64, script, updateListener);
                    removeDocument(elasticSearchId);
                }
                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId); 
                searchScrollRequest.scroll(scroll);
                searchResponse = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
            logger.info("Done!");
        }
        else {
            logger.warn("Couldn't execute search. ");
        }
        
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
