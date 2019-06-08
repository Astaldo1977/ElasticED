package de.astaldo.elastic_ed;

import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SingleDocumentUploadLambda implements Consumer<String>{
    private static final Pattern PATTERN_DOCUMENT_ID = Pattern.compile(" *\\{\"id\":(\\d+),");
    private ElasticSearchConnector client;
    
    public SingleDocumentUploadLambda(ElasticSearchConnector client) {
        this.client = client;
    }

    @Override
    public void accept(String line) {
        String d = line.substring(0, line.length() - 1);
        try {
            Matcher m = PATTERN_DOCUMENT_ID.matcher(d);
            if (m.find()) {
                client.addDocument(d, m.group(1));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
