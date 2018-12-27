package com.hanji.search;

import com.google.appengine.api.search.*;
import com.google.appengine.api.utils.SystemProperty;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(name = "HelloAppEngine", value = "/hello")
public class HelloAppEngine extends HttpServlet {

    private static final String INDEX_NAME = "words";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        Properties properties = System.getProperties();

        response.setContentType("text/plain");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().println("Hello App Engine - Standard using "
                + SystemProperty.version.get() + " Java " + properties.get("java.specification.version"));

        Document doc = Document.newBuilder()
                .addField(Field.newBuilder().setName("id").setText("가깝다0"))
                .addField(Field.newBuilder().setName("term").setText("가깝다"))
                .addField(Field.newBuilder().setName("definition").setText("to walk"))
                .build();

        try {
            indexADocument(INDEX_NAME, doc);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        doSearch(response.getWriter(), "walk");
    }

    public static String getInfo() {
        return "Version: " + System.getProperty("java.version")
                + " OS: " + System.getProperty("os.name")
                + " User: " + System.getProperty("user.name");
    }

    // From https://cloud.google.com/appengine/docs/standard/java/search/
    public static void indexADocument(String indexName, Document document)
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

    private void doSearch(PrintWriter out, String queryString) {
        final int maxRetry = 3;
        int attempts = 0;
        int delay = 2;
        while (true) {
            try {
                //String queryString = "product = piano AND price < 5000";
                Results<ScoredDocument> results = getIndex().search(queryString);

                // Iterate over the documents in the results
                for (ScoredDocument document : results) {
                    // handle results
                    out.print("id: " + document.getOnlyField("id").getText());
                    out.println(", def: " + document.getOnlyField("definition").getText());
                }
            } catch (SearchException e) {
                if (StatusCode.TRANSIENT_ERROR.equals(e.getOperationResult().getCode())
                        && ++attempts < maxRetry) {
                    // retry
                    try {
                        Thread.sleep(delay * 1000);
                    } catch (InterruptedException e1) {
                        // ignore
                    }
                    delay *= 2; // easy exponential backoff
                    continue;
                } else {
                    throw e;
                }
            }
            break;
        }
    }

    private Index getIndex() {
        IndexSpec indexSpec = IndexSpec.newBuilder().setName(INDEX_NAME).build();
        return SearchServiceFactory.getSearchService().getIndex(indexSpec);
    }

}
