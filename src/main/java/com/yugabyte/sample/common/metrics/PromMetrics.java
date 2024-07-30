package com.yugabyte.sample.common.metrics;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.log4j.Logger;

// Utility class to hit yb-tserver-ip:9000/prometheus-metrics and
// fetch counters for the relevant metrics.
public class PromMetrics {
    private final List<String> promContactPoints;

    private static final Logger LOG = Logger.getLogger(PromMetrics.class);

    /*
     * Initializes with T-Server nodes to be contacted for the metrics.
     */
    public PromMetrics(List<InetSocketAddress> nodes) throws IOException {
        promContactPoints = new ArrayList<>();
        for (InetSocketAddress node : nodes) {
            promContactPoints.add(String.format(
                "https://%s:9000/prometheus-metrics", node.getHostString()));
        }
        disableSSLVerification();
    }

    /*
     * Disable SSL since prometheus-metrics are not exposed with a valid
     * certificate.
     *
     * TODO: Figure out how to do this in a more secure way.
     */
    public static void disableSSLVerification() throws IOException {
        try {
            TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                    public void checkClientTrusted(X509Certificate[] certs, String authType) {
                    }
                    public void checkServerTrusted(X509Certificate[] certs, String authType) {
                    }
                }
            };

            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

            HostnameVerifier allHostsValid = (hostname, session) -> true;
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
        } catch (Exception e) {
            throw new IOException("Failed to disable SSL verification", e);
        }
    }

    /*
     * Fetches the counter for the given metric and table name
     * and accumulates across all the T-Servers.
     */
    public long getCounter(String metricName, String tableName) {
        long counter = 0;
        for (String promContactPoint : promContactPoints) {
            long fetchedCounter = fetchPromCounter(metricName, tableName, promContactPoint);
            if (fetchedCounter > 0) {
                counter += fetchedCounter;
            }
        }

        return counter;
    }

    /*
     * Fetches the metric counter for one T-Server.
     */
    private long fetchPromCounter(String metricName, String tableName, String promContactPoint) {
        HttpURLConnection connection = null;

        try {
            URL url = new URL(promContactPoint);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()));
                StringBuilder response = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    response.append(line).append("\n");
                }
                reader.close();

                // Example: match restart_read_requests{table_name="usertable", ...} 10 1234567890
                //                                                                    ^
                //                                                                    |
                //                                                                 counter
                Pattern pattern = Pattern.compile(
                    "^" + metricName + "\\{[^}]*table_name=\"" + tableName +
                    "\"[^}]*\\}\\s+(\\d+)\\s+(\\d+)", Pattern.MULTILINE);
                Matcher matcher = pattern.matcher(response.toString());

                if (matcher.find()) {
                    long counter = Long.parseLong(matcher.group(1));
                    if (matcher.find()) {
                        // Only one match is expected.
                        LOG.fatal("Found multiple matches for metric " +
                            metricName + " for table " + tableName);
                    }
                    return counter;
                }

                LOG.error("Failed to find metric " + metricName + " for table " + tableName);
            } else if (responseCode == HttpURLConnection.HTTP_MOVED_TEMP || responseCode == HttpURLConnection.HTTP_MOVED_PERM) {
                String newUrl = connection.getHeaderField("Location");
                LOG.info("Redirecting to " + newUrl);
                return fetchPromCounter(metricName, tableName, newUrl);
            } else {
                LOG.error("Failed to fetch metrics: HTTP response code " + responseCode);
            }
        } catch(IOException e) {
            LOG.error("Failed to fetch metrics", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }

        return -1;
    }
}
