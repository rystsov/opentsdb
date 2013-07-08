package net.opentsdb.core.index.model;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author rystsov
 * @date 6/28/13
 */
public class Era implements Comparable<Era> {
    public final Long from;
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

        era.to = null;
        eras.add(era);
        return eras;
    }

    public static Era[] filter(Era[] items, long from, Long to, long cacheTimeOut) {
        SortedSet<Era> refined = new TreeSet<Era>();
        long last = Long.MIN_VALUE;
        for (Era item : items) {
            if (last>item.from) throw new RuntimeException();
            last = item.from;
            if (to==null) {
                if (item.to!=null) {
                    if (item.to < from - 2*cacheTimeOut) continue;
                }
            } else {
                if (item.to!=null) {
                    if (item.to < from - 2*cacheTimeOut) continue;
                }
                if (item.from > to) continue;
            }
            refined.add(item);
        }
        return refined.toArray(new Era[0]);
    }

    @Override
    public int compareTo(Era o) {
        return from.compareTo(o.from);
    }
}
