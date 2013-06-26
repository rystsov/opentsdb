package net.opentsdb.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author rystsov
 * @date 6/21/13
 */
public class FederatedMetricEngine {
    private static final Logger LOG = LoggerFactory.getLogger(FederatedMetricEngine.class);
    private static final long CACHE_TIMEOUT_MS = 10*60*1000;

    private final TSDB tsdb;

    private volatile FederatedMetricIndex index;

    public FederatedMetricEngine(TSDB tsdb) {
        this.tsdb = tsdb;
        this.index = FederatedMetricIndex.load(tsdb);
    }

    public List<TsdbQueryDto> split(TsdbQueryDto query) {
        FederatedMetricIndex local = this.index;

        // TODO: adds split based on time, since query may overlap unindexed and indexed data
        List<TsdbQueryDto> result = new ArrayList<TsdbQueryDto>();
        if (local.federated.containsKey(query.metricText)) {
            FederatedMetricIndex.FederatedMetric metric = local.federated.get(query.metricText);
            FederatedMetricIndex.SubMetric subMetric = null;
            for (FederatedMetricIndex.SubMetric item : metric.subMetrics) {
                if (item.isMatch(query.tagsText)) {
                    subMetric = item;
                    break;
                }
            }
            if (subMetric==null) {
                // TODO: exclude a priori wrong sub-metrics
                for (FederatedMetricIndex.SubMetric item : metric.subMetrics) {
                    result.add(specialise(query, item));
                }
            } else {
                result.add(specialise(query, subMetric));
            }
        } else {
            result.add(query);
        }
        LOG.info("Splitted into " + result.size() + ": " + result);
        return  result;
    }

    public boolean isSubMetric(String name) {
        checkUpdateOutdatedCache();
        FederatedMetricIndex local = this.index;

        return local.subMetricsIndex.contains(name);
    }

    public String tryMapMetricToSubMetric(String metric, Map<String, String> tags) {
        checkUpdateOutdatedCache();
        FederatedMetricIndex local = this.index;

        if (!local.federated.containsKey(metric)) return metric;
        FederatedMetricIndex.SubMetric subMetric = null;
        for (FederatedMetricIndex.SubMetric item : local.federated.get(metric).subMetrics) {
            if (item.isMatch(tags)) {
                subMetric = item;
                break;
            }
        }
        if (subMetric!=null) {
            LOG.info("Remap " + metric + " to " + subMetric.name);
            metric = subMetric.name;
        }
        return metric;
    }

    private void checkUpdateOutdatedCache() {
        if (System.currentTimeMillis() - index.loadedAt > CACHE_TIMEOUT_MS) {
            index = FederatedMetricIndex.load(tsdb);
        }
    }

    private TsdbQueryDto specialise(TsdbQueryDto queue, FederatedMetricIndex.SubMetric metric) {
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
}
