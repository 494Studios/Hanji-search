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
        String cursor = request.getParameter("cursor");
        System.out.println(query);

        if(query != null) {
            String s = doSearch(query, cursor).toString();
            response.getWriter().println(s);
        } else {
            response.getWriter().println("No query given");
        }
    }

    // Based off code from https://cloud.google.com/appengine/docs/standard/java/search
    private JSONObject doSearch(String queryString, String cursorString) {
        HashSet<String> ids = new HashSet<>(); // HashSet to prevent duplicates

        final int maxRetry = 3;
        int attempts = 0;
        int delay = 2;
        Cursor cursor;
        if(cursorString == null){
            cursor = Cursor.newBuilder().build();
            System.out.println("cursor:" + cursor.toWebSafeString());
        } else {
            cursor = Cursor.newBuilder().build(cursorString);
        }

        Cursor returnCursor;
        while (true) {
            try {
                QueryOptions options = QueryOptions.newBuilder().setCursor(cursor).build();
                Query query = Query.newBuilder().setOptions(options).build(queryString);
                Results<ScoredDocument> results = getIndex().search(query);

                // Iterate over the documents in the results
                for (ScoredDocument document : results) {
                    // handle results
                    ids.add(document.getOnlyField("id").getText());
                }

                returnCursor = results.getCursor();
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

        JSONObject jo = new JSONObject();
        if(returnCursor != null) {
            jo.put("cursor", returnCursor.toWebSafeString());
        }
        jo.put("results", ids);

        return jo;
    }

    private Index getIndex() {
        IndexSpec indexSpec = IndexSpec.newBuilder().setName(IndexerEngine.INDEX_NAME).build();
        return SearchServiceFactory.getSearchService().getIndex(indexSpec);
    }
}
