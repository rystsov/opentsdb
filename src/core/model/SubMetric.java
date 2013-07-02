package net.opentsdb.core.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author rystsov
 * @date 6/27/13
 */
public class SubMetric {
    public final String name;
    public final Map<String, String> tags;

    public SubMetric(String name, Map<String, String> tags) {
        this.name = name;
        this.tags = Collections.unmodifiableMap(new HashMap<String, String>(tags));
    }

    public boolean isHead() {
        return tags.isEmpty();
    }

    public boolean isMatch(final Map<String, String> tags) {
        if (isHead()) return false;
        for (String key : this.tags.keySet()) {
            if (!tags.containsKey(key)) return false;
            if (!tags.get(key).equals(this.tags.get(key))) return false;
        }
        return true;
    }
}