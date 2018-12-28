package com.hanji.search;

import com.google.appengine.api.search.*;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
        Database db = new Database();
        try {
            Map<String,Object> data = db.fetchDoc("가하다1");
            response.getWriter().println("Added: " + data);
            addDocument(data);
        }catch (InterruptedException | ExecutionException e){
            e.printStackTrace();
        }
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

    private void addDocument(Map<String, Object> data) {
        Document.Builder builder = Document.newBuilder()
                .addField(Field.newBuilder().setName("id").setText(data.get("id").toString()))
                .addField(Field.newBuilder().setName("term").setText(data.get("term").toString()));
        ArrayList<String> definitions = (ArrayList<String>) data.get("definitions");
        for (String def : definitions) {
            builder.addField(Field.newBuilder().setName("definition").setText(def));
        }
        Document doc = builder.build();

        try {
            indexADocument(INDEX_NAME, doc);
        } catch (InterruptedException e) {
            System.err.println("Couldn't add document to index");
            e.printStackTrace();
        }
    }

    // From https://cloud.google.com/appengine/docs/standard/java/search/
    private static void indexADocument(String indexName, Document document)
            throws InterruptedException {
        IndexSpec indexSpec = IndexSpec.newBuilder().setName(indexName).build();
        Index index = SearchServiceFactory.getSearchService().getIndex(indexSpec);

        final int maxRetry = 3;
        int attempts = 0;
        int delay = 2;
        while (true) {
            try {
                index.put(document);
            } catch (PutException e) {
                if (StatusCode.TRANSIENT_ERROR.equals(e.getOperationResult().getCode())
                        && ++attempts < maxRetry) { // retrying
                    Thread.sleep(delay * 1000);
                    delay *= 2; // easy exponential backoff
                    continue;
                } else {
                    throw e; // otherwise throw
                }
            }
            break;
        }
    }

    private Index getIndex() {
        IndexSpec indexSpec = IndexSpec.newBuilder().setName(IndexerEngine.INDEX_NAME).build();
        return SearchServiceFactory.getSearchService().getIndex(indexSpec);
    }
}
