package edu.touro.cs.mcon364;

import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crawler {
    public Crawler() {
    }

    private static class Email {
        String email;
        String source;
        Date date;

        public Email() {
        }

        private Email (String email, String source, Date date) {
            this.email = email;
            this.source = source;
            this.date = date;
        }

        public String getEmail() {
            return email;
        }

        public String getSource() {
            return source;
        }

        public Date getDate() {
            return date;
        }
    }

    public static void getEmails(String url) throws IOException {
        final int EMAIL_GOAL = 10000;
        final Map<String, Email> emailMap = Collections.synchronizedMap(new HashMap<>());
        final Set<String> linkHist = Collections.synchronizedSet(new HashSet<>());
        final ConcurrentLinkedQueue<String> linkQueue = new ConcurrentLinkedQueue<>();

        ExecutorService es = new ThreadPoolExecutor(16, 16,
                20000L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1000));;

        linkQueue.add(url);
        pageScrape(linkQueue.remove(), emailMap, linkHist, linkQueue);

        while (emailMap.size() < EMAIL_GOAL && !linkQueue.isEmpty()) {
            try {
                es.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            pageScrape(linkQueue.remove(), emailMap, linkHist, linkQueue);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
            catch(Exception e){
                try {
                    Thread.sleep(1000);
                }
                catch (Exception e2){
                    e2.printStackTrace();
                }
                e.printStackTrace();
            }
        }

        es.shutdownNow();

        loadToDB(emailMap);
    }

    private static void loadToDB(Map<String, Email> emailMap) {
        final int MAX_INSERT = 1_000;

        Map<String, String> map = System.getenv();
        String endpoint = map.get("Db1Endpoint");
        String password = map.get("password");
        String url = "jdbc:sqlserver://" + endpoint+":1433" + ";"
                + "database=Roth_Dovid;"
                + "user=admin;"
                + "password=" + password +";"
                + "encrypt=false;"
                + "trustServerCertificate=false;"
                + "loginTimeout=30;";

        System.out.println("Connecting database...");

        try (Connection connection = DriverManager.getConnection(url)) {
            System.out.println("Database connected!");

            String sql = "insert into emails values (?, ?, ?)";

            PreparedStatement prepStmt = connection.prepareStatement(sql);

            Iterator<Email> it = emailMap.values().iterator();

            while (it.hasNext()) {
                for(int i=0; i<MAX_INSERT; i++) {
                    if(it.hasNext()) {
                        Email email = it.next();
                        prepStmt.setString(1, email.getEmail());
                        prepStmt.setString(2, email.getSource());
                        prepStmt.setTimestamp(3, new Timestamp(email.getDate().getTime()));
                        prepStmt.addBatch();
                    }
                }
            }

            prepStmt.executeBatch();

        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect the database!", e);
        }
    }

    private static void pageScrape(String url, Map<String, Email> emailMap, Set<String> linkHist,
                                   ConcurrentLinkedQueue<String> linkQueue) throws IOException {
        String email;
        URL absolutePassedURL = new URL(url);

        try{
            Document doc = Jsoup.connect(url).userAgent("Mozilla").get();

            Pattern p = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+");
            Matcher matcher = p.matcher(doc.text());

            while(matcher.find()) {
                if(!emailMap.containsKey(email = matcher.group())) {
                    emailMap.put(email, new Email(email, absolutePassedURL.toString(), new Date()));
                }
            }

            Elements elements = doc.select("a[href]");
            for(Element e : elements) {
                //Get link
                url = e.attr("href");

                //Extract mailtos
                if(url.contains("mailto")) {
                    emailMap.put(email = new URL(url).getPath(), new Email(email, absolutePassedURL.toString(), new Date()));
                    continue;
                }

                //Expand relative pathnames before adding to list to enable future access.
                URL newUrl = new URL(absolutePassedURL, url);

                //Skip bad paths
                if(url.contains(".pdf") || url.contains(".mp3") || url.contains(".mp4") || url.contains("google") || url.contains("youtube")) {
                    continue;
                }

                //Finally, add url
                if(linkHist.add(url)) {
                    linkQueue.add(newUrl.toString());
                }

                //Free up for gc
                newUrl = null;
            }
            //Free up for gc
            absolutePassedURL = null;
            email = null;
            doc = null;
            p = null;
            matcher = null;
            elements = null;
        }
        catch (UnsupportedMimeTypeException | HttpStatusException | MalformedURLException | SocketException | SocketTimeoutException ignored) {
        }
    }

    public static void main(String[] args) throws IOException {
        Crawler.getEmails("https://lcm.touro.edu/");
    }
}
