package com.hanji.search;

import com.google.appengine.api.search.*;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Akash Eldo (axe1412)
 */
@WebServlet(name = "IndexerEngine", value = "/index")
public class IndexerEngine extends HttpServlet {

    public static final String INDEX_NAME = "words";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/plain");
        response.setCharacterEncoding("UTF-8");

        response.getWriter().println("Deleted: " + clearIndex() + " documents from index");
    }

    private int clearIndex(){
        int count = 0;
        try {
            // looping because getRange by default returns up to 100 documents at a time
            while (true) {
                List docIds = new ArrayList<>();
                // Return a set of doc_ids.
                GetRequest request = GetRequest.newBuilder().setReturningIdsOnly(true).build();
                GetResponse response = getIndex().getRange(request);
                if (response.getResults().isEmpty()) {
                    break;
                }
                for (Object doc : response) {
                    docIds.add(((Document)doc).getId());
                    count++;
                }
                getIndex().delete(docIds);
            }
            // Delete the index schema
            getIndex().deleteSchema();
        } catch (RuntimeException e) {
            System.out.println("Failed to delete index");
        }
        return count;
    }

    private Index getIndex() {
        IndexSpec indexSpec = IndexSpec.newBuilder().setName(IndexerEngine.INDEX_NAME).build();
        return SearchServiceFactory.getSearchService().getIndex(indexSpec);
    }
}
