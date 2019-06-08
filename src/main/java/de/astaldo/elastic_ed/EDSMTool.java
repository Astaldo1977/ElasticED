package de.astaldo.elastic_ed;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import de.astaldo.elastic_ed.elastic_search.ScrollListener;

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
//        ElasticSearchConnector systems = new ElasticSearchConnector(host, CORE_SYSTEMS);
        ElasticSearchConnector bodies  = new ElasticSearchConnector(host, CORE_BODIES);
        
        SearchRequest searchRequestBodies = new SearchRequest(CORE_SYSTEMS);
        SearchSourceBuilder builder = new SearchSourceBuilder();
//                builder.query();
//                        QueryBuilders.boolQuery()
//                        .mustNot(
//                                QueryBuilders.existsQuery("system.coords"))
//                        .must(
//                                QueryBuilders.existsQuery("systemName")))
//                .size(10000); 
        searchRequestBodies
            .source(builder)
            .scroll(TimeValue.timeValueMinutes(1L));
        bodies.scrollAll(searchRequestBodies, new ScrollListener());
    }
}
