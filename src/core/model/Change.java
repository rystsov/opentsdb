package net.opentsdb.core.model;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author rystsov
 * @date 6/27/13
 */
public class Change implements Comparable<Change> {
    public Long ts;
    public Map<String, String> tags;

    public String subMetricName(String root) {
        SortedMap<String, String> tags = new TreeMap<String, String>(this.tags);

        String subMetricName = root;
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            subMetricName += "/" + tag.getKey() + "/" + tag.getValue();
        }

        return subMetricName;
    }

    @Override
    public int compareTo(Change o) {
        return ts.compareTo(o.ts);
    }
}
