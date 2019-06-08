package de.astaldo.elastic_ed.elastic_search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;

public class ScrollListener implements ActionListener<SearchResponse> {
    private int count = 1;
    
    @Override
    public void onResponse(SearchResponse response) {
        System.out.println("Received response " + count++);
    }

    @Override
    public void onFailure(Exception e) {
        System.err.println(e);
    }
}
