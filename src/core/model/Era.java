package net.opentsdb.core.model;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author rystsov
 * @date 6/28/13
 */
public class Era implements Comparable<Era> {
    public Long from;
    public Long to;
    public FederatedMetric metric;

    public Era(long from, FederatedMetric metric) {
        this.from = from;
        this.metric = metric;
    }

    public static SortedSet<Era> build(String metric, SortedSet<Change> changes) {
        SortedSet<Era> eras = new TreeSet<Era>();

        Era era = new Era(Long.MIN_VALUE, new FederatedMetric(metric));

        for(Change change : changes) {
            era.to = change.ts;
            eras.add(era);
            era = new Era(change.ts, era.metric.apply(change));
        }

        era.to = Long.MAX_VALUE;
        eras.add(era);
        return eras;
    }

    public static SortedSet<Era> filter(List<Era> items, long from, long to, long cacheTimeOut) {
        SortedSet<Era> refined = new TreeSet<Era>();
        for (Era item : items) {
            if (item.to < from - 2*cacheTimeOut) continue;
            if (item.from > to) continue;
            refined.add(item);
        }
        return refined;
    }

    @Override
    public int compareTo(Era o) {
        return from.compareTo(o.from);
    }
}
