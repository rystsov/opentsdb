package net.opentsdb.core.index;

import net.opentsdb.core.index.model.Change;
import net.opentsdb.core.index.model.Era;
import net.opentsdb.core.index.model.FederatedMetric;

import java.util.Collection;
import java.util.HashMap;
import java.util.SortedSet;

/**
 * @author rystsov
 * @date 7/4/13
 */
public interface Index {
    boolean isFederated(String name);

    FederatedMetric getFederatedMetric(String metric);

    SortedSet<Change> getChanges(String name);

    Era[] getAscEra(String name);

    Collection<FederatedMetric> list();

    long snapshotTS();

    void addIndex(String metric, HashMap<String, String> tags);

    void removeIndex(String metric, HashMap<String, String> tags);
}
