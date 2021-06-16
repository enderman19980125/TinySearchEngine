package crawl;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedWriter;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Crawl implements Runnable {
    static private Path rootPath, waitPath, finishPath;
    static private Logger logger;
    static private ExecutorService fixedThreadPool;
    static private String regexURL = "";

    static private int secondsSinceLastAddURL = 0;
    static private final int SECONDS_WAIT_TO_STOP = 60;

    static private int fileIndex = 0, itemIndex = 0;
    static private final int ITEMS_PER_FILE = 10000;

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
     * Set the regex for filtering URLs, e.g. "^https?://[^/]*njnu.edu.cn.*$".
     *
     * @param regex a regex string
     */
    static public void setFilterPattern(String regex) {
        regexURL = regex;
    }

    /**
     * Initialize rootPath and urlsPath.
     *
     * @param rootPath root path to save the whole content
     * @throws IOException IOException
     */
    static private void initializePath(String rootPath) throws IOException {
        Crawl.rootPath = Paths.get(rootPath);
        Crawl.waitPath = Crawl.rootPath.resolve(".wait");
        Crawl.finishPath = Crawl.rootPath.resolve(".finish");
        if (!Files.exists(Crawl.rootPath)) Files.createDirectories(Crawl.rootPath);
        if (!Files.exists(Crawl.waitPath)) Files.createDirectories(Crawl.waitPath);
        if (!Files.exists(Crawl.finishPath)) Files.createDirectories(Crawl.finishPath);
    }

    /**
     * Initialize logger with console handler and file handler.
     * The log file is located in "${rootPath}/scrapy.log".
     *
     * @throws IOException IOException
     */
    static private void initializeLogger() throws IOException {
        logger = Logger.getLogger("crawl");

        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);

        Handler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.FINE);
        logger.addHandler(consoleHandler);

        String filename = rootPath.resolve("crawl.log").toString();
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
        List<String> urlsList = loadURLs();

        for (String url : urlsList) {
            fixedThreadPool.execute(new Crawl(url));
        }

        while (secondsSinceLastAddURL <= SECONDS_WAIT_TO_STOP) {
            TimeUnit.SECONDS.sleep(10);
            secondsSinceLastAddURL += 10;
            if (secondsSinceLastAddURL >= 20) {
                String msg = String.format("%s seconds have passed since last adding URL.", secondsSinceLastAddURL);
                logger.finest(msg);
            }
        }

        fixedThreadPool.shutdown();
        logger.info("Thread pool has shutdown.");

        while (!fixedThreadPool.isTerminated()) {
            TimeUnit.SECONDS.sleep(1);
        }

        logger.info("Thread pool has terminated.");
    }

    /**
     * Load URLs from disk. The files are located in "${rootPath}/.wait".
     *
     * @return a list of URLs to be crawled
     * @throws IOException IOException
     */
    static protected List<String> loadURLs() throws IOException {
        List<String> urlsList = new LinkedList<>();
        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(waitPath);

        for (Path subDirectory : directoryStream) {
            DirectoryStream<Path> subdirectoryStream = Files.newDirectoryStream(subDirectory);
            for (Path file : subdirectoryStream) {
                Scanner scanner = new Scanner(file);
                while (scanner.hasNextLine()) {
                    String url = scanner.nextLine();
                    if (!url.equals("")) urlsList.add(url);
                }
            }
        }

        return urlsList;
    }

    /**
     * Add URL to "${rootPath}/.wait", then add URL to thread pool.
     *
     * @param currentURL current URL
     * @param nextURL    URL to be added
     */
    static protected void addURL(String currentURL, String nextURL) {
        Path waitFilePath = generateWaitPath(nextURL);
        Path waitParentPath = waitFilePath.getParent();

        try {
            if (!Files.exists(waitParentPath)) Files.createDirectories(waitParentPath);
        } catch (IOException e) {
            // e.printStackTrace();
        }

        try {
            Files.createFile(waitFilePath);
            FileWriter fileWriter = new FileWriter(waitFilePath.toString());
            BufferedWriter writer = new BufferedWriter(fileWriter);
            writer.write(nextURL);
            writer.close();
        } catch (FileAlreadyExistsException e) {
            // e.printStackTrace();
            String msg = String.format("%s [addURL] %s already existed.", currentURL, nextURL);
            logger.finest(msg);
            return;
        } catch (IOException e) {
            // e.printStackTrace();
            String msg = String.format("%s [addURL] %s failed to add!", currentURL, nextURL);
            logger.severe(msg);
            return;
        }

        fixedThreadPool.execute(new Crawl(nextURL));
        secondsSinceLastAddURL = 0;
        String msg = String.format("%s [addURL] %s success.", currentURL, nextURL);
        logger.finest(msg);
    }

    /**
     * Move URL from "${rootPath}/.wait" to "${rootPath}/.finish".
     *
     * @param url URL to be removed
     */
    static protected void finishURL(String url) {
        Path waitFilePath = generateWaitPath(url);
        Path finishFilePath = generateFinishPath(url);
        Path finishParentPath = finishFilePath.getParent();

        try {
            if (!Files.exists(finishParentPath)) Files.createDirectories(finishParentPath);
        } catch (IOException e) {
            // e.printStackTrace();
        }

        try {
            Files.move(waitFilePath, finishFilePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            // e.printStackTrace();
            String msg = String.format("%s [finishURL] failed to move!", url);
            logger.severe(msg);
            return;
        }

        String msg = String.format("%s [finishURL] success.", url);
        logger.finest(msg);
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
            // e.printStackTrace();
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
     * Filter URLs within domain "*.njnu.edu.cn" and remove URL suffix "#xxx".
     *
     * @param urlsList a list of URLs
     * @return a list of filtered URLs
     */
    static protected List<String> filterURLs(List<String> urlsList) {
        List<String> filteredURLsList = new LinkedList<>();
        for (String url : urlsList) {
            if (url.matches(regexURL)) {
                int k = url.lastIndexOf("#");
                if (k > 0) url = url.substring(0, k);
                filteredURLsList.add(url);
            }
        }
        return filteredURLsList;
    }

    /**
     * Generate wait path for the specific URL.
     *
     * @param url URL
     * @return wait path like "${rootPath}/.wait/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
     */
    static private Path generateWaitPath(String url) {
        String uuid = UUID.nameUUIDFromBytes(url.getBytes()).toString();
        return waitPath.resolve(uuid.substring(0, 4)).resolve(uuid);
    }

    /**
     * Generate finish path for the specific URL.
     *
     * @param url URL
     * @return finish path like "${rootPath}/.finish/xxx/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
     */
    static private Path generateFinishPath(String url) {
        String uuid = UUID.nameUUIDFromBytes(url.getBytes()).toString();
        return finishPath.resolve(uuid.substring(0, 4)).resolve(uuid);
    }

    /**
     * Check whether URL exists or not.
     *
     * @param url URL to be checked
     * @return true if exists, false otherwise
     */
    static private boolean isExistURL(String url) {
        Path finishFilePath = generateFinishPath(url);
        return Files.exists(finishFilePath);
    }

    /**
     * Save current webpage to disk.
     *
     * @param url      URL
     * @param nextURLs a list of URLs from current URL
     * @param title    title of current webpage
     * @param body     body of current webpage
     * @return true if success, false otherwise
     */
    static private synchronized boolean save(String url, List<String> nextURLs, String title, String body) {
        if (itemIndex >= ITEMS_PER_FILE) {
            ++fileIndex;
            itemIndex = 0;
        }

        StringBuilder contentBuilder = new StringBuilder();

        contentBuilder.append("[").append(itemIndex).append("]");
        contentBuilder.append("\t");

        contentBuilder.append("[URL]").append("\t");
        contentBuilder.append(url).append("\t");

        contentBuilder.append("[nextURLs]").append("\t");
        for (String nextURL : nextURLs) contentBuilder.append(nextURL).append(" ");
        contentBuilder.append("\t");

        contentBuilder.append("[title]").append("\t");
        title = title.replaceAll("[\t\n]", " ").replaceAll(" +", " ").trim();
        contentBuilder.append(title).append("\t");

        contentBuilder.append("[body]").append("\t");
        body = body.replaceAll("[\t\n]", " ").replaceAll(" +", " ").trim();
        contentBuilder.append(body).append("\n");

        String content = contentBuilder.toString();
        Path filePath = rootPath.resolve(String.format("part-%d", fileIndex));

        try {
            if (!Files.exists(filePath)) Files.createFile(filePath);
            FileWriter fileWriter = new FileWriter(filePath.toString(), true);
            BufferedWriter writer = new BufferedWriter(fileWriter);
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            // e.printStackTrace();
            return false;
        }

        ++itemIndex;

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
     * (3) Parse all valid URLs in the webpage, then add to "${rootPath}/.wait".
     * (4) Move current URL from "${rootPath}/.wait" to "${rootPath}/.finish".
     */
    @Override
    public void run() {
        String msg = String.format("%s starts ...", url);
        logger.finest(msg);

        Document document = request(url);
        if (document == null) {
            finishURL(url);
            msg = String.format("%s [Request] failed to request!", url);
            logger.finest(msg);
            return;
        }

        String title = parseTitle(document);
        String body = parseBody(document);
        List<String> urlsList = filterURLs(parseURLs(document));

        msg = String.format("%s\n[title] %s\n[body] %s", url, title, body);
        logger.finest(msg);

        if (!save(url, urlsList, title, body)) {
            msg = String.format("%s failed to save!", url);
            logger.severe(msg);
            return;
        }

        for (String nextURL : urlsList)
            if (!isExistURL(nextURL) && !url.equals(nextURL)) {
                addURL(url, nextURL);
            }

        finishURL(url);
        msg = String.format("%s success.", url);
        logger.info(msg);
    }
}