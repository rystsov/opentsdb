package net.opentsdb.core.index;

import net.opentsdb.core.TsdbQueryDto;
import org.joda.time.DateTimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author rystsov
 * @date 7/5/13
 */
public class TestBase {
    protected long now;
    protected MemoryTSDB tsdb;
    protected MemoryIndex.MemoryIndexLoader loader;
    protected FederatedMetricEngine engine;
    protected final long dt = 10000;

    protected void init() {
        now = System.currentTimeMillis() / 1000;
        now = now * 1000;
        DateTimeUtils.setCurrentMillisFixed(now);

        tsdb = new MemoryTSDB();
        loader = new MemoryIndex.MemoryIndexLoader(tsdb);
        engine = new FederatedMetricEngine(loader, tsdb, dt);
    }

    protected TsdbQueryDto queue(String metric, long from, long to, String tags) {
        return queue(metric, from, to, parseTags(tags));
    }

    protected TsdbQueryDto queue(String metric, long from, long to, Map<String, String> tags) {
        TsdbQueryDto query = new TsdbQueryDto();
        query.start_time = from;
        query.end_time = to;
        query.metricText = metric;
        query.tagsText = tags;
        return query;
    }

    protected void incTime(long dt) {
        now += dt;
        DateTimeUtils.setCurrentMillisFixed(now);
    }

    protected void mkMetric(String metric) {
        tsdb.getOrCreateMetric(metric);
    }

    protected void addMeasure(String metric, long time, long value, String tags) {
        Map<String, String> parsedTag = parseTags(tags);
        metric = engine.tryMapMetricToSubMetric(metric, parsedTag);
        tsdb.put(metric, time, value, parsedTag);
    }

    protected HashMap<String, String> parseTags(String tags) {
        if (tags==null) return new HashMap<String, String>();
        HashMap<String, String> parsedTag = new HashMap<String, String>();
        String[] keyValue = tags.split(" ");
        for (int i=0;i<keyValue.length;i++) {
            String[] parts = keyValue[i].split("=");
            parsedTag.put(parts[0], parts[1]);
        }
        return parsedTag;
    }

    protected boolean containsOnlyOnce(List<MemoryTSDB.Measure> measures, long value) {
        boolean has = false;
        for (MemoryTSDB.Measure measure : measures) {
            if (has && measure.value==value) return false;
            if (measure.value==value) has = true;
        }
        return has;
    }
}
