package net.opentsdb.core.index;

import net.opentsdb.core.TsdbQueryDto;

import java.util.*;

/**
 * @author rystsov
 * @date 7/5/13
 */
public class MemoryTSDB implements IdResolver {
    public static class Measure implements Comparable<Measure> {
        public final Long time;
        public final long value;
        public final Map<String, String> tags;

        public Measure(long time, long value, Map<String, String> tags) {
            this.time = time;
            this.value = value;
            this.tags = tags;
        }

        @Override
        public int compareTo(Measure o) {
            return time.compareTo(o.time);
        }
    }

    private int id=0;
    private Map<String, byte[]> storage = new HashMap<String, byte[]>();

    @Override
    public byte[] getMetric(String metric) {
        if (!storage.containsKey(metric)) {
            throw new RuntimeException();
        }
        return storage.get(metric);
    }

    @Override
    public byte[] getOrCreateMetric(String metric) {
        if (!storage.containsKey(metric)) {
            int id = this.id++;
            storage.put(metric, new byte[]{
                    (byte)(id >>> 16),
                    (byte)(id >>> 8),
                    (byte)id
            });
            records.put(metric, new TreeSet<Measure>());
        }
        return storage.get(metric);
    }

    private Map<String, SortedSet<Measure>> records = new HashMap<String, SortedSet<Measure>>();

    public void put(String metric, long time, long value, Map<String, String> tags) {
        records.get(metric).add(new Measure(time, value, tags));
    }

    public List<Measure> query(List<TsdbQueryDto> queries) {
        List<Measure> measures = new ArrayList<Measure>();
        for (TsdbQueryDto query : queries) {
            outer: for (Measure measure : records.get(query.metricText)) {
                if (measure.time < query.start_time*1000) continue;
                if (measure.time > query.end_time*1000) break;

                for (String key : query.tagsText.keySet()) {
                    if (!measure.tags.containsKey(key)) continue outer;
                    if (!measure.tags.get(key).equals(query.tagsText.get(key))) continue outer;
                }
                measures.add(measure);
            }
        }
        return measures;
    }
}
