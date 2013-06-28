package net.opentsdb.core.model;

import net.opentsdb.core.FederatedMetricIndex;

import java.util.*;

/**
 * @author rystsov
 * @date 6/27/13
 */
public class FederatedMetric {
    public final String metric;
    public final List<SubMetric> subMetrics;

    public FederatedMetric(String metric) {
        List<SubMetric> core = new ArrayList<SubMetric>();
        core.add(new SubMetric(metric, new HashMap<String, String>()));
        this.metric = metric;
        this.subMetrics = Collections.unmodifiableList(core);
    }

    private FederatedMetric(String metric, List<SubMetric> subMetrics) {
        this.metric = metric;
        this.subMetrics = Collections.unmodifiableList(subMetrics);
    }

    public static FederatedMetric create(String metric, SortedSet<Change> changes) {
        if (changes==null) return null;
        FederatedMetric result = new FederatedMetric(metric);
        for (Change change : changes) {
            result = result.apply(change);
        }
        return result;
    }

    public FederatedMetric apply(Change change) {
        String subMetricName = change.subMetricName(metric);

        for (SubMetric item : subMetrics) {
            if (item.name.equals(subMetricName)) {
                throw new RuntimeException("SubMetric " + subMetricName + " alread exists");
            }
        }

        List<SubMetric> nova = new ArrayList<SubMetric>(subMetrics);
        nova.add(new SubMetric(subMetricName, change.tags));
        return new FederatedMetric(metric, nova);
    }
}
