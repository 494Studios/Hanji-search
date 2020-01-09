package com.hanji.search;

import com.google.appengine.api.search.*;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@WebServlet(name = "IndexEngine", value = "/index")
public class IndexEngine  extends HttpServlet{
    private Database db;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/plain");
        response.setCharacterEncoding("UTF-8");
        String id = request.getParameter("id");
        System.out.println(id);

        db = new Database();
        try {
            Map<String,Object> doc =  db.fetchDoc(id);
            addDocument(doc, response);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }


    }

    private void addDocument(Map<String, Object> data, HttpServletResponse response) throws IOException {
        Document.Builder builder = Document.newBuilder()
                .addField(Field.newBuilder().setName("id").setText(data.get("id").toString()))
                .addField(Field.newBuilder().setName("term").setText(data.get("term").toString()));
        ArrayList<String> definitions = (ArrayList<String>) data.get("definitions");
        for (String def : definitions) {
            builder.addField(Field.newBuilder().setName("definition").setText(def));
        }

        try {
            builder.setId(URLEncoder.encode(data.get("id").toString(),"UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        Document doc = builder.build();

        try {
            indexADocument(ClearEngine.INDEX_NAME, doc);
            response.getWriter().println("SUCCESS: " + doc.getOnlyField("id"));
        } catch (InterruptedException e) {
            response.sendError(500, Arrays.toString(e.getStackTrace()));
        }
    }

    // From https://cloud.google.com/appengine/docs/standard/java/search/
    private void indexADocument(String indexName, Document document) throws InterruptedException {
        IndexSpec indexSpec = IndexSpec.newBuilder().setName(indexName).build();
        Index index = SearchServiceFactory.getSearchService().getIndex(indexSpec);

        final int maxRetry = 3;
        int attempts = 0;
        int delay = 2;
        while (true) {
            try {
                index.put(document);

                // Getting id
                String id = null;
                for(Field f: document.getFields("id")){
                    id = f.getText();
                }
                db.markDocAsIndexed(id);

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
