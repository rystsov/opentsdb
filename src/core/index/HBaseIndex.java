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
public class HBaseIndex extends IndexTemplate {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseIndex.class);

    public static class Loader implements IndexLoader {
        private final HBaseClient client;
        private final byte[] indextable;
        private final IdResolver resolver;

        public Loader(final IdResolver resolver, HBaseClient client, byte[] indextable) {
            this.client = client;
            this.indextable = indextable;
            this.resolver = resolver;
        }

        @Override
        public Index load() {
            HBaseIndex index = new HBaseIndex(resolver, client, indextable);
            index.load();
            return index;
        }
    }

    private static final byte[] TSDB_INDEX_CF = "submetrics".getBytes();
    private static final byte[] TSDB_INDEX_Q = "all".getBytes();

    private static final short MAX_ATTEMPTS_PUT = 6;
    private static final short INITIAL_EXP_BACKOFF_DELAY = 800;
    private final HBaseClient client;
    private final byte[] indextable;

    protected HBaseIndex(IdResolver resolver, HBaseClient client, byte[] indextable) {
        super(resolver);
        this.client = client;
        this.indextable = indextable;
    }

    @Override
    protected Map<String, SortedSet<Change>> scan() {
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

    @Override
    protected void putChanges(String metric, SortedSet<Change> changes) {
        String json = new Gson().toJson(changes);

        putWithRetry(new PutRequest(
                        indextable, metric.getBytes(),
                        TSDB_INDEX_CF, TSDB_INDEX_Q, json.getBytes()
        ), MAX_ATTEMPTS_PUT, INITIAL_EXP_BACKOFF_DELAY);
    }

    private org.hbase.async.Scanner getScanner() throws HBaseException {
        final org.hbase.async.Scanner scanner = client.newScanner(indextable);
        scanner.setFamily(TSDB_INDEX_CF);
        return scanner;
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
