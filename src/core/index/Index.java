package net.opentsdb.core.index;

import net.opentsdb.core.index.model.Change;
import net.opentsdb.core.index.model.FederatedMetric;

import java.util.Collection;
import java.util.SortedSet;

/**
 * @author rystsov
 * @date 7/4/13
 */
public interface Index {
    boolean isFederated(String name);

    FederatedMetric getFederatedMetric(String metric);

    void putChanges(String metric, SortedSet<Change> changes);

    SortedSet<Change> getChanges(String name);

    Collection<FederatedMetric> list();

    long snapshotTS();
}
