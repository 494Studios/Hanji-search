package com.hanji.search;

import com.google.appengine.api.search.*;
import org.json.JSONObject;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;

/**
 * @author Akash Eldo (axe1412)
 */
@WebServlet(name = "SearchEngine", value = "/search")
public class SearchEngine extends HttpServlet{

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/plain");
        response.setCharacterEncoding("UTF-8");
        String query = request.getParameter("query");
        if(query != null) {
            String s = JSONObject.valueToString(doSearch(query));
            response.getWriter().println(s);
        } else {
            response.getWriter().println("No query given");
        }
    }

    // Based off code from https://cloud.google.com/appengine/docs/standard/java/searc
    private HashSet<String> doSearch(String queryString) {
        HashSet<String> ids = new HashSet<>(); // HashSet to prevent duplicates

        final int maxRetry = 3;
        int attempts = 0;
        int delay = 2;
        while (true) {
            try {
                Results<ScoredDocument> results = getIndex().search(queryString);

                // Iterate over the documents in the results
                for (ScoredDocument document : results) {
                    // handle results
                    ids.add(document.getOnlyField("id").getText());
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

        return ids;
    }

    private Index getIndex() {
        IndexSpec indexSpec = IndexSpec.newBuilder().setName(IndexerEngine.INDEX_NAME).build();
        return SearchServiceFactory.getSearchService().getIndex(indexSpec);
    }
}
