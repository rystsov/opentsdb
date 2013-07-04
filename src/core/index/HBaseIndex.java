package net.opentsdb.core.index;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import net.opentsdb.core.index.model.Change;
import net.opentsdb.core.index.model.FederatedMetric;
import org.hbase.async.*;
import org.joda.time.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author rystsov
 * @date 6/25/13
 */
public class HBaseIndex implements Index {
    public static class Loader implements IndexLoader {
        private final HBaseClient client;
        private final byte[] indextable;

        public Loader(HBaseClient client, byte[] indextable) {
            this.client = client;
            this.indextable = indextable;
        }

        @Override
        public Index load() {
            return HBaseIndex.load(client, indextable);
        }
    }

    private static final byte[] TSDB_INDEX_CF = "submetrics".getBytes();
    private static final byte[] TSDB_INDEX_Q = "all".getBytes();

    private static final short MAX_ATTEMPTS_PUT = 6;
    /**
     * Initial delay in ms for exponential backoff to retry failed RPCs.
     */
    private static final short INITIAL_EXP_BACKOFF_DELAY = 800;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndex.class);

    private final HBaseClient client;
    private final byte[] indextable;


    private long loadedAt;
    private Map<String, SortedSet<Change>> index;
    private Map<String, FederatedMetric> head;
    private HashSet<String> subMetricsIndex;


    private static HBaseIndex load(final HBaseClient client, final byte[] indextable) {
        HBaseIndex loaded = new HBaseIndex(client, indextable);
        loaded.index = loaded.scan();
        loaded.initSubMetricIndex();
        loaded.fillHead();
        loaded.loadedAt = DateTimeUtils.currentTimeMillis();
        return loaded;
    }

    @Override
    public boolean isFederated(String name) {
        return index.containsKey(name);
    }

    @Override
    public FederatedMetric getFederatedMetric(String metric) {
        return head.get(metric);
    }

    @Override
    public void putChanges(String metric, SortedSet<Change> changes) {
        String json = new Gson().toJson(changes);

        putWithRetry(new PutRequest(
                        indextable, metric.getBytes(),
                        TSDB_INDEX_CF, TSDB_INDEX_Q, json.getBytes()
        ), MAX_ATTEMPTS_PUT, INITIAL_EXP_BACKOFF_DELAY);
    }

    @Override
    public SortedSet<Change> getChanges(String name) {
        return index.containsKey(name) ? index.get(name) : null;
    }

    @Override
    public Collection<FederatedMetric> list() {
        List<FederatedMetric> result = new ArrayList<FederatedMetric>();
        for (String key : index.keySet()) {
            result.add(FederatedMetric.create(key, index.get(key)));
        }
        return result;
    }

    @Override
    public long snapshotTS() {
        return loadedAt;
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
        final org.hbase.async.Scanner scanner = client.newScanner(indextable);
        scanner.setFamily(TSDB_INDEX_CF);
        return scanner;
    }



    private HBaseIndex(final HBaseClient client, final byte[] indextable) {
        this.client = client;
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
                client.put(put).joinUninterruptibly();
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
