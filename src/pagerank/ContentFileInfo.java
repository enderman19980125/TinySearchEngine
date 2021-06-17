package pagerank;

import java.util.*;

public class ContentFileInfo {
    private String URL = "URL", title = "title", body = "body";
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

    public ContentFileInfo(String content) {
        StringTokenizer stringTokenizer = new StringTokenizer(content, "\t", true);

        while (stringTokenizer.hasMoreTokens()) {
            String line = stringTokenizer.nextToken();
            if (line.equals("[URL]")) {
                stringTokenizer.nextToken();
                URL = stringTokenizer.nextToken();
                continue;
            }
            if (line.equals("[nextURLs]")) {
                stringTokenizer.nextToken();
                String urls = stringTokenizer.nextToken();
                if (urls.equals("\t")) continue;
                StringTokenizer urlsTokenizer = new StringTokenizer(urls, " ");
                while (urlsTokenizer.hasMoreTokens()) {
                    nextURLs.add(urlsTokenizer.nextToken());
                }
                continue;
            }
            if (line.equals("[title]")) {
                stringTokenizer.nextToken();
                title = stringTokenizer.nextToken();
                continue;
            }
            if (line.equals("[body]")) {
                if (stringTokenizer.hasMoreTokens()) stringTokenizer.nextToken();
                if (stringTokenizer.hasMoreTokens()) body = stringTokenizer.nextToken();
            }
        }
    }
}