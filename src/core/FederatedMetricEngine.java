package net.opentsdb.core;

import net.opentsdb.core.model.Era;
import net.opentsdb.core.model.FederatedMetric;
import net.opentsdb.core.model.SubMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author rystsov
 * @date 6/21/13
 */
public class FederatedMetricEngine {
    private static final Logger LOG = LoggerFactory.getLogger(FederatedMetricEngine.class);
    //private static final long CACHE_TIMEOUT_MS = 10*60*1000;
    //TODO: change CACHE_TIMEOUT_MS
    private static final long CACHE_TIMEOUT_MS = 1000;

    private final TSDB tsdb;
    private final byte[] indextable;

    private volatile FederatedMetricIndex index;

    public FederatedMetricEngine(TSDB tsdb, byte[] indextable) {
        this.tsdb = tsdb;
        this.indextable = indextable;
        this.index = FederatedMetricIndex.load(tsdb, indextable);
    }

    public List<TsdbQueryDto> split(TsdbQueryDto query) {
        FederatedMetricIndex local = this.index;

        List<TsdbQueryDto> result = new ArrayList<TsdbQueryDto>();
        if (local.index.containsKey(query.metricText)) {
            SortedSet<Era> eras = Era.build(query.metricText, local.index.get(query.metricText));
            eras = Era.filter(eras, query.start_time*1000, query.end_time==null?null:query.end_time*1000, CACHE_TIMEOUT_MS);

            for (Era era : eras) {
                List<SubMetric> subMetrics = new ArrayList<SubMetric>();
                for (SubMetric item : era.metric.subMetrics) {
                    if (item.isMatch(query.tagsText)) {
                        subMetrics.add(item);
                    }
                }
                if (subMetrics.size()==0) {
                    // TODO: exclude a priori wrong sub-metrics
                    for (SubMetric item : era.metric.subMetrics) {
                        result.add(specialise(query, era, item));
                    }
                } else {
                    for (SubMetric item : subMetrics) {
                        result.add(specialise(query, era, item));
                    }
                }
            }
        } else {
            result.add(query);
        }
        result = dedupAll(result);
        String info =
                "#########################################\n" +
                "Splitted into " + result.size() + " parts";
        for (TsdbQueryDto item : result) {
            info += "\n" + item.toString();
        }
        info += "\n#########################################";
        LOG.info(info);
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

        if (!local.head.containsKey(metric)) return metric;
        SubMetric subMetric = null;
        for (SubMetric item : local.head.get(metric).subMetrics) {
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
            index = FederatedMetricIndex.load(tsdb, indextable);
        }
    }

    private TsdbQueryDto specialise(TsdbQueryDto queue, Era era, SubMetric metric) {
        TsdbQueryDto nova = queue.clone();
        nova.metric = tsdb.metrics.getId(metric.name);
        nova.metricText = metric.name;
        nova.start_time = Math.max(queue.start_time*1000, era.from);
        if (queue.end_time != null) {
            if (era.to!=null) {
                nova.end_time = Math.min(era.to+2*CACHE_TIMEOUT_MS, queue.end_time*1000);
            } else {
                nova.end_time = queue.end_time*1000;
            }
        } else {
            if (era.to!=null) {
                nova.end_time = era.to+2*CACHE_TIMEOUT_MS;
            } else {
                nova.end_time = null;
            }
        }
        nova.start_time /= 1000;
        if (nova.end_time != null) nova.end_time /= 1000;
        return nova;
    }

    private static class Par implements Comparable<Par> {
        public final boolean isOpen;
        public final Long time;

        private Par(boolean open, Long time) {
            isOpen = open;
            this.time = time;
            if (time==null) throw new RuntimeException();
        }

        public void putTo(SortedMap<Long, List<Par>> openClosePar) {
            if (!openClosePar.containsKey(time)) openClosePar.put(time, new ArrayList<Par>());
            openClosePar.get(time).add(this);
        }

        @Override
        public int compareTo(Par o) {
            return time.compareTo(o.time);
        }
    }

    public static List<TsdbQueryDto> dedupAll(List<TsdbQueryDto> queries) {
        Map<String, List<TsdbQueryDto>> grouped = new HashMap<String, List<TsdbQueryDto>>();
        for (TsdbQueryDto query : queries) {
            if (!grouped.containsKey(query.metricText)) {
                grouped.put(query.metricText, new ArrayList<TsdbQueryDto>());
            }
            grouped.get(query.metricText).add(query);
        }
        List<TsdbQueryDto> result = new ArrayList<TsdbQueryDto>();
        for(List<TsdbQueryDto> group : grouped.values()) {
            result.addAll(dedupTrack(group));
        }
        return result;
    }

    public static List<TsdbQueryDto> dedupTrack(List<TsdbQueryDto> queries) {
        String metric = null;
        SortedMap<Long, List<Par>> openClosePar = new TreeMap<Long, List<Par>>();
        for(TsdbQueryDto query : queries) {
            if (metric==null) metric = query.metricText;
            if (!metric.equals(query.metricText)) throw new RuntimeException();

            new Par(true, query.start_time).putTo(openClosePar);
            new Par(false, query.end_time==null ? Long.MAX_VALUE : query.end_time).putTo(openClosePar);
        }

        TsdbQueryDto template = queries.get(0);
        List<TsdbQueryDto> result = new ArrayList<TsdbQueryDto>();
        Long last = null;
        int opened = 0;
        for (Map.Entry<Long, List<Par>> item : openClosePar.entrySet()) {
            for (Par par : item.getValue()) {
                if (par.isOpen) opened++;
                if (!par.isOpen) opened--;
            }
            if (opened<0) throw new RuntimeException();
            if (last==null) {
                if (opened==0) {
                    TsdbQueryDto query = template.clone();
                    query.start_time = item.getKey();
                    query.end_time = item.getKey();
                    result.add(query);
                } else {
                    last = item.getKey();
                }
            } else {
                if (opened==0) {
                    TsdbQueryDto query = template.clone();
                    query.start_time = last;
                    query.end_time = item.getKey();
                    result.add(query);
                    last = null;
                }
            }
        }
        for (TsdbQueryDto query : result) {
            if (query.end_time==Long.MAX_VALUE) query.end_time = null;
        }
        return result;
    }
}
