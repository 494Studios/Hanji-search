package com.hanji.search;

import com.google.appengine.api.search.*;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

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
            doSearch(response.getWriter(), query);
        } else {
            response.getWriter().println(query);
        }
    }

    private void doSearch(PrintWriter out, String queryString) {
        final int maxRetry = 3;
        int attempts = 0;
        int delay = 2;
        while (true) {
            try {
                Results<ScoredDocument> results = getIndex().search(queryString);

                // Iterate over the documents in the results
                for (ScoredDocument document : results) {
                    // handle results
                    out.print("id: " + document.getOnlyField("id").getText());
                    out.println(", def: " + document.getFields("definition"));
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
        IndexSpec indexSpec = IndexSpec.newBuilder().setName(IndexerEngine.INDEX_NAME).build();
        return SearchServiceFactory.getSearchService().getIndex(indexSpec);
    }
}
