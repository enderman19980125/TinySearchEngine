package pagerank;

import java.util.*;

public class ContentFileInfo {
    private String URL, title, body;
    private final List<String> nextURLs = new LinkedList<>();

    public String getURL() {
        return URL;
    }

    public List<String> getNextURLs() {
        return nextURLs;
    }

    public String getTitle() {
        return title;
    }

    public String getBody() {
        return body;
    }

    ContentFileInfo(String content) {
        StringTokenizer contentTokenizer = new StringTokenizer(content, "\n");
        while (contentTokenizer.hasMoreTokens()) {
            String line = contentTokenizer.nextToken();
            if (line.equals("[URL]")) {
                URL = contentTokenizer.nextToken();
                continue;
            }
            if (line.equals("[nextURLs]")) {
                String urls = contentTokenizer.nextToken();
                StringTokenizer urlsTokenizer = new StringTokenizer(urls, " ");
                while (urlsTokenizer.hasMoreTokens()) {
                    nextURLs.add(urlsTokenizer.nextToken());
                }
                continue;
            }
            if (line.equals("[title]")) {
                title = contentTokenizer.nextToken();
                continue;
            }
            if (line.equals("[body]")) {
                body = contentTokenizer.nextToken();
            }
        }
    }
}