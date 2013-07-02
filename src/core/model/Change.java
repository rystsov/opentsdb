package net.opentsdb.core.model;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author rystsov
 * @date 6/27/13
 */
public class Change implements Comparable<Change> {
    public enum ChangeType {ADD, REMOVE}

    public Long ts;
    public ChangeType type;
    public Map<String, String> tags;

    public String subMetricName(String root) {
        return subMetricName(tags, root);
    }

    @Override
    public int compareTo(Change o) {
        return ts.compareTo(o.ts);
    }

    protected String subMetricName(Map<String, String> tags, String root) {
        tags = new TreeMap<String, String>(tags);

        String subMetricName = root;
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            subMetricName += "/" + tag.getKey() + "/" + tag.getValue();
        }

        return subMetricName;
    }
}
