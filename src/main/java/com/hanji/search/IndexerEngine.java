package com.hanji.search;

import com.google.appengine.api.search.*;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
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

        Database db = new Database();
        try {
            Map<String,Object> data = db.fetchDoc("가하다1");
            response.getWriter().println(data);
            //addDocument(data);
        }catch (InterruptedException | ExecutionException e){
            e.printStackTrace();
        }
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
}
