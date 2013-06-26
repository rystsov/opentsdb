package net.opentsdb.core;

import com.google.gson.Gson;
import com.stumbleupon.async.Deferred;
import org.hbase.async.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Scanner;

/**
 * @author rystsov
 * @date 6/25/13
 */
public class FederatedMetricIndex {
    public static final byte[] TSDB_INDEX_CF = "submetrics".getBytes();
    public static final byte[] TSDB_INDEX_Q = "all".getBytes();

    private static final short MAX_ATTEMPTS_PUT = 6;
    /**
     * Initial delay in ms for exponential backoff to retry failed RPCs.
     */
    private static final short INITIAL_EXP_BACKOFF_DELAY = 800;

    private static final Logger LOG = LoggerFactory.getLogger(FederatedMetricIndex.class);

    public static class FederatedMetric {
        public String metric;

        public List<SubMetric> subMetrics = new ArrayList<SubMetric>();
    }

    public static class SubMetric {
        public String name;
        public Map<String, String> tags;

        public SubMetric() {
            tags = new HashMap<String, String>();
        }

        public boolean isHead() {
            return tags.isEmpty();
        }

        public boolean isMatch(final Map<String, String> tags) {
            if (isHead()) return false;
            for (String key : this.tags.keySet()) {
                if (!tags.containsKey(key)) return false;
                if (!tags.get(key).equals(this.tags.get(key))) return false;
            }
            return true;
        }
    }

    public final TSDB tsdb;
    final byte[] indextable;
    public Map<String, FederatedMetric> federated;
    public HashSet<String> subMetricsIndex;
    public long loadedAt;

    public static FederatedMetricIndex load(final TSDB tsdb, final byte[] indextable) {
        FederatedMetricIndex loaded = new FederatedMetricIndex(tsdb, indextable);
        loaded.federated = loaded.scan();
        loaded.initSubMetricIndex();
        loaded.loadedAt = System.currentTimeMillis();
        return loaded;
    }


    public void put(FederatedMetric metric) {
        String json = new Gson().toJson(metric);

        putWithRetry(new PutRequest(
                        indextable, metric.metric.getBytes(),
                        TSDB_INDEX_CF, TSDB_INDEX_Q, json.getBytes()
        ), MAX_ATTEMPTS_PUT, INITIAL_EXP_BACKOFF_DELAY);
    }

    public FederatedMetric get(String name) {
        return federated.containsKey(name) ? federated.get(name) : null;
    }

    public Collection<FederatedMetric> list() {
        return Collections.unmodifiableCollection(federated.values());
    }


    private Map<String, FederatedMetric> scan() {
        Map<String, FederatedMetric> metrics = new HashMap<String, FederatedMetric>();
        final org.hbase.async.Scanner scanner = getScanner();
        ArrayList<ArrayList<KeyValue>> rows;
        try {
            while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    if (row.size()!=1) throw new RuntimeException("Corrupted index");

                    FederatedMetric metric = new Gson().fromJson(
                            new String(row.get(0).value()),
                            FederatedMetric.class
                    );
                    metrics.put(metric.metric, metric);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return metrics;
    }

    private org.hbase.async.Scanner getScanner() throws HBaseException {
        final org.hbase.async.Scanner scanner = tsdb.client.newScanner(indextable);
        scanner.setFamily(TSDB_INDEX_CF);
        return scanner;
    }



    private FederatedMetricIndex(final TSDB tsdb, final byte[] indextable) {
        this.tsdb = tsdb;
        this.indextable = indextable;
    }

    private void initSubMetricIndex() {
        subMetricsIndex = new HashSet<String>();
        for (FederatedMetric metric : federated.values()) {
            for (SubMetric sub : metric.subMetrics) {
                if (sub.isHead()) continue;
                subMetricsIndex.add(sub.name);
            }
        }
    }

    private void putWithRetry(final PutRequest put, short attempts, short wait) throws HBaseException {
        put.setBufferable(false);  // TODO(tsuna): Remove once this code is async.
        while (attempts-- > 0) {
            try {
                tsdb.client.put(put).joinUninterruptibly();
                return;
            } catch (HBaseException e) {
                if (attempts > 0) {
                    LOG.error("Put failed, attempts left=" + attempts
                            + " (retrying in " + wait + " ms), put=" + put, e);
                    try {
                        Thread.sleep(wait);
                    } catch (InterruptedException ie) {
                        throw new RuntimeException("interrupted", ie);
                    }
                    wait *= 2;
                } else {
                    throw e;
                }
            } catch (Exception e) {
                LOG.error("WTF?  Unexpected exception type, put=" + put, e);
            }
        }
        throw new IllegalStateException("This code should never be reached!");
    }
}
