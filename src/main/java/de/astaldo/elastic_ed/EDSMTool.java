package de.astaldo.elastic_ed;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class EDSMTool {
    public final static String CORE_BODIES = "edsm-bodies";
    public final static String CORE_SYSTEMS = "edsm-systems";
    
    private static final Log logger = LogFactory.getLog(EDSMTool.class);
    
    public static void main(String[] args) throws IOException{
        new EDSMTool().go();
    }
    private void go() throws IOException {
//        uploadDocuments(Paths.get("e:/Downloads/EDSM","bodies.json"),  "127.0.0.1", CORE_BODIES,  0);
//        uploadDocuments(Paths.get("e:/Downloads/EDSM","systems.json"), "127.0.0.1", CORE_SYSTEMS, 0);
        mapBodiesToSystems("127.0.0.1");
    }

    public void uploadDocuments(Path path, String host, String core, long skipItems) throws IOException {
        
        if (path.toFile().exists()) {
            ElasticSearchConnector client = new ElasticSearchConnector(host, core);
            
            SingleDocumentUploadLambda uploadLambda = new SingleDocumentUploadLambda(client);
            
            Stream<String> stream = Files.lines(path);
            stream.parallel()
                .skip(skipItems)
                .filter(line -> line.charAt(0) != '[' && line.charAt(0) != ']')
                .forEach(uploadLambda);
            stream.close();
            client.flushPending();
        }
    }
    private void mapBodiesToSystems(String host) throws IOException {
        final String systemCoordsKey = "systemCoordinates"; 
        final int scrollSize = 1000;
        final Scroll scrollTime = new Scroll(TimeValue.timeValueMinutes(1L));
        //
        // Build classes
        //
        ElasticSearchConnector systems = new ElasticSearchConnector(host, CORE_SYSTEMS);
        ElasticSearchConnector bodies  = new ElasticSearchConnector(host, CORE_BODIES);
        
        SearchRequest searchRequestBodies  = new SearchRequest(CORE_BODIES);
        SearchRequest searchRequestSystems = new SearchRequest(CORE_SYSTEMS);
        
        SearchSourceBuilder builderBodies  = new SearchSourceBuilder();
        SearchSourceBuilder builderSystems = new SearchSourceBuilder();
        
        //
        // Build Query Builders
        //
        builderBodies.query(
            QueryBuilders.boolQuery()
                .mustNot(
                        QueryBuilders.existsQuery(systemCoordsKey)
                )
                .must(
                        QueryBuilders.existsQuery("systemName")
                )
            )
            .sort(new FieldSortBuilder("systemName.keyword").order(SortOrder.ASC))
            .size(scrollSize);
        
        builderSystems.query(
                QueryBuilders.matchAllQuery())
            .sort(new FieldSortBuilder("name.keyword").order(SortOrder.ASC))
            .size(scrollSize);
        
        searchRequestBodies
            .source(builderBodies)
            .scroll(scrollTime);
        searchRequestSystems
            .source(builderSystems)
            .scroll(scrollTime);

        SearchResponse searchResponseBodies  = bodies.getClient().search(searchRequestBodies,   RequestOptions.DEFAULT);
        SearchResponse searchResponseSystems = systems.getClient().search(searchRequestSystems, RequestOptions.DEFAULT);
        if(
                   searchResponseBodies.status()  == RestStatus.OK 
                && searchResponseSystems.status() == RestStatus.OK
                && searchResponseBodies.getScrollId()  != null 
                && searchResponseSystems.getScrollId() != null) {
            
            String scrollIdBodies = searchResponseBodies.getScrollId();
            String scrollIdSystems = searchResponseSystems.getScrollId();
            
            SearchHit[] searchHitsBodies  = searchResponseBodies.getHits().getHits();
            SearchHit[] searchHitsSystems = searchResponseSystems.getHits().getHits();
            
            int indexBody   = 0;
            int indexSystem = 0;
            
            logger.info("Starting to scroll through.");
            do {
                String systemName         =  (String) searchHitsSystems[indexSystem].getSourceAsMap().get("name");
                String systemNameFromBody =  (String) searchHitsBodies[indexBody].getSourceAsMap().get("systemName");
                
                int comparison = systemName.compareTo(systemNameFromBody);
                if(comparison == 0) {
                    Map<String, Object> coords = new HashMap<>();
                    coords.put(systemCoordsKey, searchHitsSystems[indexSystem].getSourceAsMap().get("coords"));
                    do {
                        bodies.updateDocument(searchHitsBodies[indexBody].getId(), coords);
                        indexBody++;
                    }
                    while(indexBody < searchHitsBodies.length && systemNameFromBody.equals(searchHitsBodies[indexBody].getSourceAsMap().get("systemName")));
                }
                else if(comparison > 0) {
                    indexBody++;
                }
                else {
                    indexSystem++;
                }

                if(indexBody >= searchHitsBodies.length) {
                    SearchScrollRequest searchScrollRequestBodies = new SearchScrollRequest(scrollIdBodies); 
                    searchScrollRequestBodies.scroll(scrollTime);
                    searchResponseBodies = bodies.getClient().scroll(searchScrollRequestBodies, RequestOptions.DEFAULT);
                    scrollIdBodies = searchResponseBodies.getScrollId();
                    searchHitsBodies = searchResponseBodies.getHits().getHits();
                    if (searchResponseBodies.status()  != RestStatus.OK ||  searchResponseBodies.getScrollId() == null){
                        logger.info("no more bodies to parse");
                        break;
                    }
                    indexBody = 0;
                }
                else if(indexSystem >= searchHitsSystems.length) {
                    SearchScrollRequest searchScrollRequestSystems = new SearchScrollRequest(scrollIdSystems); 
                    searchScrollRequestSystems.scroll(scrollTime);
                    searchResponseSystems = systems.getClient().scroll(searchScrollRequestSystems, RequestOptions.DEFAULT);
                    scrollIdSystems = searchResponseSystems.getScrollId();
                    searchHitsSystems = searchResponseSystems.getHits().getHits();
                    if (searchResponseSystems.status()  != RestStatus.OK ||  searchResponseSystems.getScrollId() == null){
                        logger.info("no more systems to parse");
                        break;
                    }
                    indexSystem = 0;
                }
            }
            while(true);

        }
    }
}
