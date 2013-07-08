package net.opentsdb.core.index;

import net.opentsdb.core.index.model.Change;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author rystsov
 * @date 7/5/13
 */
public class MemoryIndex extends IndexTemplate {
    public static class MemoryIndexLoader implements IndexLoader {
        public final Map<String, SortedSet<Change>> core = new HashMap<String, SortedSet<Change>>();
        public final IdResolver idResolver;

        public MemoryIndexLoader(IdResolver idResolver) {
            this.idResolver = idResolver;
        }

        @Override
        public Index load() {
            MemoryIndex index = new MemoryIndex(idResolver, core);
            index.load();
            return index;
        }
    }

    private final Map<String, SortedSet<Change>> core;

    protected MemoryIndex(IdResolver resolver, Map<String, SortedSet<Change>> core) {
        super(resolver);
        this.core = core;
    }

    @Override
    protected Map<String, SortedSet<Change>> scan() {
        Map<String, SortedSet<Change>> result = new HashMap<String, SortedSet<Change>>();
        for (String key : core.keySet()) {
            result.put(key, new TreeSet<Change>(core.get(key)));
        }
        return result;
    }

    @Override
    protected void putChanges(String metric, SortedSet<Change> changes) {
        core.put(metric, new TreeSet<Change>(changes));
    }
}
