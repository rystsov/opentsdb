package net.opentsdb.core;

import java.util.*;

/**
 * @author rystsov
 * @date 6/21/13
 */
public class FederatedMetricEngine {
    static class FederatedMetric {
        public String metric;

        public List<SubMetric> subMetrics;


    }

    static class SubMetric {
        public String name;
        public Map<String, String> tags;

        public SubMetric() {
            tags = new HashMap<String, String>();
        }

        public boolean isHead() {
            return tags.isEmpty();
        }

        public boolean isMatch(TsdbQueryDto query) {
            if (isHead()) return false;
            for(String key : tags.keySet()) {
                if (!query.tagsText.containsKey(key)) return false;
                if (!query.tagsText.get(key).equals(tags.get(key))) return false;
            }
            return true;
        }
    }

    private final TSDB tsdb;
    private Map<String, FederatedMetric> federated;
    private HashSet<String> subMetricsIndex;

    public FederatedMetricEngine(TSDB tsdb) {
        this.tsdb = tsdb;
        this.federated = new HashMap<String, FederatedMetric>();

        FederatedMetric stuff = new FederatedMetric();
        stuff.metric = "stuff";
        stuff.subMetrics = new ArrayList<SubMetric>();

        SubMetric clusterA = new SubMetric();
        clusterA.tags.put("cluster", "a");
        clusterA.name = "stuff/cluster/a";

        SubMetric clusterB = new SubMetric();
        clusterB.tags.put("cluster", "b");
        clusterB.name = "stuff/cluster/b";

        SubMetric head = new SubMetric();
        head.name = "stuff";

        stuff.subMetrics.add(clusterA);
        stuff.subMetrics.add(clusterB);
        stuff.subMetrics.add(head);

        federated.put(stuff.metric, stuff);
        initSubMetricIndex();
    }

    public List<TsdbQueryDto> split(TsdbQueryDto query) {
        List<TsdbQueryDto> result = new ArrayList<TsdbQueryDto>();
        if (federated.containsKey(query.metricText)) {
            FederatedMetric metric = federated.get(query.metricText);
            SubMetric subMetric = null;
            for (SubMetric item : metric.subMetrics) {
                if (item.isMatch(query)) {
                    subMetric = item;
                    break;
                }
            }
            if (subMetric==null) {
                for (SubMetric item : metric.subMetrics) {
                    result.add(specialise(query, item));
                }
            } else {
                result.add(specialise(query, subMetric));
            }
        } else {
            result.add(query);
        }
        return  result;
    }

    public boolean isSubMetric(String name) {
        return subMetricsIndex.contains(name);
    }

    TsdbQueryDto specialise(TsdbQueryDto queue, SubMetric metric) {
        TsdbQueryDto nova = new TsdbQueryDto();
        nova.metric = tsdb.metrics.getId(metric.name);
        nova.metricText = metric.name;
        nova.start_time = queue.start_time;
        nova.end_time = queue.end_time;
        nova.aggregator = queue.aggregator;
        nova.rate = queue.rate;
        nova.downsampler = queue.downsampler;
        nova.sample_interval = queue.sample_interval;
        nova.group_by_values = queue.group_by_values;
        nova.group_bys = queue.group_bys;
        nova.tags = queue.tags;
        nova.tagsText = queue.tagsText;
        return nova;
    }

    void initSubMetricIndex() {
        subMetricsIndex = new HashSet<String>();
        for (FederatedMetric metric : federated.values()) {
            for (SubMetric sub : metric.subMetrics) {
                if (sub.isHead()) continue;
                subMetricsIndex.add(sub.name);
            }
        }
    }
}