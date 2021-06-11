package crawl;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.DirectoryStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Crawl implements Runnable {
    static private Path rootPath, urlsPath;
    static private Logger logger;
    static private ExecutorService fixedThreadPool;

    private final String url;

    /**
     * Initialize path, logger and thread pool.
     *
     * @param numThreads num of threads in thread pool
     * @param rootPath   root path to save the whole content
     * @throws IOException IOException
     */
    static public void initialize(int numThreads, String rootPath) throws IOException {
        initializePath(rootPath);
        initializeLogger();
        initializeThreadPool(numThreads);
    }

    /**
     * Initialize rootPath and urlsPath.
     *
     * @param rootPath root path to save the whole content
     * @throws IOException IOException
     */
    static private void initializePath(String rootPath) throws IOException {
        Crawl.rootPath = Paths.get(rootPath);
        Crawl.urlsPath = Crawl.rootPath.resolve(".urls");
        if (!Files.exists(Crawl.rootPath))
            Files.createDirectories(Crawl.rootPath);
        if (!Files.exists(Crawl.urlsPath))
            Files.createDirectories(Crawl.urlsPath);
    }

    /**
     * Initialize logger with console handler and file handler.
     * The log file is located in "${rootPath}/scrapy.log".
     *
     * @throws IOException IOException
     */
    static private void initializeLogger() throws IOException {
        logger = Logger.getLogger("scrapy");

        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);

        Handler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINE);
        logger.addHandler(consoleHandler);

        String filename = rootPath.resolve("scrapy.log").toString();
        Handler fileHandler = new FileHandler(filename, true);
        fileHandler.setLevel(Level.INFO);
        logger.addHandler(fileHandler);
    }

    /**
     * Initialize fixed thread pool.
     *
     * @param numThreads num of threads in thread pool
     */
    static private void initializeThreadPool(int numThreads) {
        fixedThreadPool = Executors.newFixedThreadPool(numThreads);
    }

    /**
     * Start to crawl.
     *
     * @throws IOException          IOException
     * @throws InterruptedException InterruptedException
     */
    static public void start() throws IOException, InterruptedException {
        List<String> urlsLisk = loadURLs();

        for (String url : urlsLisk) {
            fixedThreadPool.execute(new Crawl(url));
        }

        while (!fixedThreadPool.isTerminated()) {
            TimeUnit.SECONDS.sleep(10);
        }

        fixedThreadPool.shutdown();
    }

    /**
     * Load URLs from disk. The files are located in "${rootPath}/.urls".
     *
     * @return a list of URLs to be crawled
     * @throws IOException IOException
     */
    static protected List<String> loadURLs() throws IOException {
        List<String> urlsList = new LinkedList<>();
        DirectoryStream<Path> stream = Files.newDirectoryStream(urlsPath);
        for (Path file : stream) {
            Scanner scanner = new Scanner(file);
            String url = scanner.nextLine();
            urlsList.add(url);
        }
        return urlsList;
    }

    /**
     * Add URL to "${rootPath}/.urls", then add URL to thread pool.
     *
     * @param url URL to be added
     * @return true if exists or success, false otherwise
     */
    static protected boolean addURL(String url) {
        String uuid = UUID.nameUUIDFromBytes(url.getBytes()).toString();
        Path urlPath = urlsPath.resolve(uuid);

        if (Files.exists(urlPath))
            return true;

        try {
            Files.createFile(urlPath);
            FileWriter fileWriter = new FileWriter(urlPath.toString());
            BufferedWriter writer = new BufferedWriter(fileWriter);
            writer.write(url);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        fixedThreadPool.execute(new Crawl(url));

        return true;
    }

    /**
     * Remove URL from "${rootPath}/.urls".
     *
     * @param url URL to be removed
     * @return true if non-exists or success, false otherwise
     */
    static protected boolean removeURL(String url) {
        String uuid = UUID.nameUUIDFromBytes(url.getBytes()).toString();
        Path urlPath = urlsPath.resolve(uuid);

        if (Files.notExists(urlPath))
            return true;

        try {
            Files.delete(urlPath);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * Request the webpage of current URL.
     *
     * @param url URL
     * @return webpage object
     */
    static protected Document request(String url) {
        Document document = null;
        try {
            document = Jsoup.connect(url).get();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return document;
    }

    /**
     * Parse title of current webpage.
     *
     * @param document webpage object
     * @return title content
     */
    static protected String parseTitle(Document document) {
        return document.title();
    }

    /**
     * Parse body of current webpage.
     *
     * @param document webpage object
     * @return body content
     */
    static protected String parseBody(Document document) {
        return document.text();
    }

    /**
     * Parse all URLs in current webpage.
     *
     * @param document webpage object
     * @return a list of URLs
     */
    static protected List<String> parseURLs(Document document) {
        List<String> urlsList = new LinkedList<>();
        Elements elements = document.select("a[href]");
        for (Element e : elements) {
            String url = e.attr("abs:href");
            if (url.equals("")) continue;
            urlsList.add(url);
        }
        return urlsList;
    }

    /**
     * Filter URLs within domain "*.njnu.edu.cn".
     *
     * @param urlsList a list of URLs
     * @return a list of filtered URLs
     */
    static protected List<String> filterURLs(List<String> urlsList) {
        List<String> filteredURLsList = new LinkedList<>();
        for (String url : urlsList) {
            if (url.matches("^https?://[^/]*njnu.edu.cn.*$"))
                filteredURLsList.add(url);
        }
        return filteredURLsList;
    }

    /**
     * Generate the saving path for the specific URL.
     *
     * @param url URL
     * @return saving path
     */
    static private Path generateSavingPath(String url) {
        url = url.replaceAll("^https?://", "");
        return rootPath.resolve(url);
    }

    /**
     * Check whether URL exists or not.
     *
     * @param url URL to be checked
     * @return true if exists, false otherwise
     */
    static private boolean isExistURL(String url) {
        Path contentPath = generateSavingPath(url).resolve("content");
        return Files.exists(contentPath);
    }

    /**
     * Save current webpage to disk.
     *
     * @param savingPath saving path
     * @param url        URL
     * @param title      title of current webpage
     * @param body       body of current webpage
     * @return true if success, false otherwise
     */
    static private boolean save(Path savingPath, String url, String title, String body) {
        Path contentPath = savingPath.resolve("content");
        String content = String.format("%s\n%s\n%s\n", url, title, body);

        try {
            if (!Files.exists(savingPath))
                Files.createDirectories(savingPath);
            if (Files.exists(contentPath))
                Files.delete(contentPath);
            Files.createFile(contentPath);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        try {
            FileWriter fileWriter = new FileWriter(contentPath.toString());
            BufferedWriter writer = new BufferedWriter(fileWriter);
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    /**
     * Constructor.
     *
     * @param url URL
     */
    public Crawl(String url) {
        this.url = url;
    }

    /**
     * Accomplish the whole process of checking, crawling, parsing and saving.
     * The process involves 5 steps:
     * (1) Crawl webpage.
     * (2) Parse title and body, then save to disk.
     * (3) Parse all valid URLs in the webpage, then add to "${rootPath}/.urls".
     * (4) Remove current URL from "${rootPath}/.urls".
     */
    @Override
    public void run() {
        String msg = String.format("%s starts ...", url);
        logger.fine(msg);

        Document document = request(url);
        if (document == null) {
            msg = String.format("%s [Request] failed to request!", url);
            logger.severe(msg);
            return;
        }

        String title = parseTitle(document);
        String body = parseBody(document);
        Path savingPath = generateSavingPath(url);

        msg = String.format("%s\n[title] %s\n[body] %s\n[savingPath] %s", url, title, body, savingPath);
        logger.finest(msg);

        if (!save(savingPath, url, title, body)) {
            msg = String.format("%s failed to save!", url);
            logger.severe(msg);
            return;
        }

        List<String> urlsList = filterURLs(parseURLs(document));
        for (String nextURL : urlsList)
            if (!isExistURL(nextURL)) {
                if (addURL(nextURL)) {
                    msg = String.format("%s [addURL] %s success.", url, nextURL);
                    logger.finest(msg);
                } else {
                    msg = String.format("%s [addURL] %s failed to add!", url, nextURL);
                    logger.severe(msg);
                    return;
                }
            }

        if (removeURL(url)) {
            msg = String.format("%s [removeURL] success.", url);
            logger.finest(msg);
        } else {
            msg = String.format("%s [removeURL] failed to remove!", url);
            logger.severe(msg);
            return;
        }

        msg = String.format("%s success.", url);
        logger.info(msg);
    }
}