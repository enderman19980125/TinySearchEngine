package crawl;

import java.io.*;
import java.util.*;

import org.jsoup.nodes.Document;

public class Main {
    static private void demo() {
        Document document = Crawl.request("http://www.njnu.edu.cn/");
        String title = Crawl.parseTitle(document);
        String body = Crawl.parseBody(document);
        List<String> linksList = Crawl.parseURLs(document);
        List<String> filteredLinksList = Crawl.filterURLs(Arrays.asList(
                "http://njnu.edu.cn/",
                "https://njnu.edu.cn/",
                "http://www.njnu.edu.cn",
                "https://www.njnu.edu.cn",
                "http://www.njnu.edu.cn/",
                "https://www.njnu.edu.cn/",
                "http://sub.njnu.edu.cn/",
                "http://sub.njnu.edu.cn/abc",
                "http://sub.njnu.edu.cn/abc/123",
                "http://sub.nju.edu.cn/",
                "http://sub.nju.edu.cn/abc",
                "http://sub.nju.edu.cn/abc/123"
        ));
        System.out.println("[title]");
        System.out.println(title);
        System.out.println("[body]");
        System.out.println(body);
        System.out.println("[linksList]");
        System.out.println(linksList);
        System.out.println("[filteredLinksList]");
        System.out.println(filteredLinksList);
    }

    static public void main(String[] args) throws IOException, InterruptedException {
        // demo();

        boolean isFirstTime = false;
        int numThreads = 16;
        String rootPath = null;

        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("--first")) {
                isFirstTime = true;
                continue;
            }
            if (args[i].equals("--num")) {
                numThreads = Integer.parseInt(args[++i]);
                continue;
            }
            if (args[i].equals("--path")) {
                rootPath = args[++i];
            }
        }

        Crawl.initialize(numThreads, rootPath);
        if (isFirstTime) Crawl.addURL("", "https://news.njnu.edu.cn/");
        Crawl.start();
    }
}