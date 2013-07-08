package net.opentsdb.core.index;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import net.opentsdb.core.index.model.Change;
import net.opentsdb.core.index.model.Era;
import net.opentsdb.core.index.model.FederatedMetric;
import org.hbase.async.HBaseClient;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.joda.time.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author rystsov
 * @date 6/25/13
 */
public abstract class IndexTemplate implements Index {
    private static final Logger LOG = LoggerFactory.getLogger(IndexTemplate.class);

    protected final IdResolver resolver;

    protected long loadedAt;
    protected Map<String, SortedSet<Change>> index;
    protected Map<String, FederatedMetric> head;
    protected Map<String, Era[]> eras;

    protected IndexTemplate(final IdResolver resolver) {
        this.resolver = resolver;
    }

    protected void load() {
        index = scan();
        fillHead();
        fillEras();
        loadedAt = DateTimeUtils.currentTimeMillis();
    }

    @Override
    public boolean isFederated(String name) {
        return index.containsKey(name);
    }

    @Override
    public Era[] getAscEra(String name) {
        return eras.get(name);
    }

    @Override
    public FederatedMetric getFederatedMetric(String metric) {
        return head.get(metric);
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

    @Override
    public void addIndex(String metric, HashMap<String, String> tags) {
        alterIndex(Change.ChangeType.ADD, metric, tags);
    }

    @Override
    public void removeIndex(String metric, HashMap<String, String> tags) {
        alterIndex(Change.ChangeType.REMOVE, metric, tags);
    }

    protected void alterIndex(Change.ChangeType type, String metric, HashMap<String, String> tags) {
        Change change = new Change();
        change.type = type;
        change.tags = new HashMap<String, String>();
        change.ts = DateTimeUtils.currentTimeMillis();
        change.tags = tags;

        SortedSet<Change> changes = isFederated(metric) ? getChanges(metric) : new TreeSet<Change>();
        changes.add(change);

        // check change is consistent
        FederatedMetric.create(metric, changes);

        resolver.getOrCreateMetric(change.subMetricName(metric));

        putChanges(metric, changes);
    }

    protected void fillHead() {
        head = new HashMap<String, FederatedMetric>();
        for (String key : index.keySet()) {
            head.put(key, FederatedMetric.create(key, index.get(key)));
        }
    }

    protected void fillEras() {
        eras = new HashMap<String, Era[]>();
        for (String key : index.keySet()) {
            eras.put(key, Era.build(key, getChanges(key)).toArray(new Era[0]));
        }
    }

    protected abstract Map<String, SortedSet<Change>> scan();
    protected abstract void putChanges(String metric, SortedSet<Change> changes);
}
