package net.opentsdb.core;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.model.Change;
import net.opentsdb.core.model.FederatedMetric;
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




    public final TSDB tsdb;
    final byte[] indextable;

    public long loadedAt;

    public Map<String, SortedSet<Change>> index;
    public Map<String, FederatedMetric> head;
    public HashSet<String> subMetricsIndex;


    public static FederatedMetricIndex load(final TSDB tsdb, final byte[] indextable) {
        FederatedMetricIndex loaded = new FederatedMetricIndex(tsdb, indextable);
        loaded.index = loaded.scan();
        loaded.initSubMetricIndex();
        loaded.fillHead();
        loaded.loadedAt = System.currentTimeMillis();
        return loaded;
    }


    public void put(String metric, SortedSet<Change> changes) {
        String json = new Gson().toJson(changes);

        putWithRetry(new PutRequest(
                        indextable, metric.getBytes(),
                        TSDB_INDEX_CF, TSDB_INDEX_Q, json.getBytes()
        ), MAX_ATTEMPTS_PUT, INITIAL_EXP_BACKOFF_DELAY);
    }

    public SortedSet<Change> get(String name) {
        return index.containsKey(name) ? index.get(name) : null;
    }

    public Collection<FederatedMetric> list() {
        List<FederatedMetric> result = new ArrayList<FederatedMetric>();
        for (String key : index.keySet()) {
            result.add(FederatedMetric.create(key, index.get(key)));
        }
        return result;
    }


    private Map<String, SortedSet<Change>> scan() {
        Map<String, SortedSet<Change>> metrics = new HashMap<String, SortedSet<Change>>();
        final org.hbase.async.Scanner scanner = getScanner();
        ArrayList<ArrayList<KeyValue>> rows;
        try {
            while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
                for (final ArrayList<KeyValue> row : rows) {
                    if (row.size()!=1) throw new RuntimeException("Corrupted index");

                    List<Change> changes = new Gson().fromJson(
                        new String(row.get(0).value()),
                        new TypeToken<List<Change>>() {}.getType()
                    );

                    metrics.put(
                            new String(row.get(0).key()),
                            new TreeSet<Change>(changes));
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

        for (String key : index.keySet()) {
            for (Change sub : index.get(key)) {
                if (sub.type== Change.ChangeType.ADD) {
                    subMetricsIndex.add(sub.subMetricName(key));
                }
            }
        }
    }

    private void fillHead() {
        head = new HashMap<String, FederatedMetric>();
        for (String key : index.keySet()) {
            head.put(key, FederatedMetric.create(key, index.get(key)));
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
