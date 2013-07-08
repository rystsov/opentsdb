package net.opentsdb.core.index;

import net.opentsdb.core.TsdbQueryDto;
import net.opentsdb.core.TsdbQueryDtoBuilder;
import org.joda.time.DateTimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.*;
import static org.hamcrest.CoreMatchers.*;

import java.util.*;

/**
 * @author rystsov
 * @date 7/5/13
 */
public class TestFederatedMetricSplit extends TestBase {
    @Before
    public void init() {
        super.init();
    }

    @Test
    public void splitless(){
        String metric = "foo";
        mkMetric(metric);

        long from = now;
        addMeasure(metric, now, 1, "host=local");
        incTime(1000);

        List<TsdbQueryDto> queries = engine.split(queue(metric, from / 1000, now / 1000, (String)null));
        Assert.assertEquals(1, queries.size());
        List<MemoryTSDB.Measure> measures = tsdb.query(queries);
        Assert.assertEquals(1, measures.size());
        Assert.assertEquals(1, measures.get(0).value);
    }

    @Test
    public void ADHD(){
        String metric = "foo";
        mkMetric(metric);

        long from = now;
        addMeasure(metric, now, 1, "host=local");
        incTime(1000);

        Index index = loader.load();
        index.addIndex(metric, parseTags("cluster=a"));
        FederatedMetricEngine concurrent = new FederatedMetricEngine(loader, tsdb, dt);
        HashMap<String, String> tags = parseTags("cluster=a");
        String submetric = concurrent.tryMapMetricToSubMetric(metric, now, tags);
        Assert.assertNotEquals(metric, submetric);

        addMeasure(submetric, now, 2, "cluster=a host=local");
        incTime(1000);
        Assert.assertTrue(now < from + dt);

        List<TsdbQueryDto> queries = engine.split(queue(metric, from / 1000, now / 1000, "cluster=a"));
        Assert.assertEquals(1, queries.size());
        List<MemoryTSDB.Measure> measures = tsdb.query(queries);
        Assert.assertEquals(0, measures.size());
    }

    @Test
    public void assiduity(){
        String metric = "foo";
        mkMetric(metric);

        long from = now;
        addMeasure(metric, now, 1, "host=local");
        incTime(1000);

        Index index = loader.load();
        index.addIndex(metric, parseTags("cluster=a"));
        FederatedMetricEngine concurrent = new FederatedMetricEngine(loader, tsdb, dt);
        HashMap<String, String> tags = parseTags("cluster=a");
        String submetric = concurrent.tryMapMetricToSubMetric(metric, now, tags);
        Assert.assertNotEquals(metric, submetric);

        addMeasure(submetric, now, 2, "cluster=a host=local");
        incTime(dt);
        Assert.assertFalse(now < from + dt);

        List<TsdbQueryDto> queries = engine.split(queue(metric, from / 1000, now / 1000, "cluster=a"));
        Assert.assertEquals(2, queries.size());
        List<MemoryTSDB.Measure> measures = tsdb.query(queries);
        Assert.assertEquals(1, measures.size());
        Assert.assertEquals(2, measures.get(0).value);
    }

    @Test
    public void trivial() {
        String metric = "foo";
        mkMetric(metric);

        long from = now;
        addMeasure(metric, now, 1, "host=local");
        incTime(1000);

        Index index = loader.load();
        index.addIndex(metric, parseTags("cluster=a"));
        HashMap<String, String> tags = parseTags("cluster=a");
        String submetric = engine.tryMapMetricToSubMetric(metric, now, tags);
        Assert.assertEquals(metric, submetric);
        addMeasure(submetric, now, 2, "cluster=a host=local");
        incTime(dt);

        tags = parseTags("cluster=a");
        submetric = engine.tryMapMetricToSubMetric(metric, now, tags);
        Assert.assertNotEquals(metric, submetric);
        addMeasure(submetric, now, 3, "cluster=a host=local");
        incTime(1000);

        List<TsdbQueryDto> queries = engine.split(queue(metric, from / 1000, now / 1000, "cluster=a"));
        Assert.assertEquals(2, queries.size());
        List<MemoryTSDB.Measure> measures = tsdb.query(queries);
        Assert.assertEquals(2, measures.size());
        Assert.assertFalse(containsOnlyOnce(measures, 1));
        Assert.assertTrue(containsOnlyOnce(measures, 2));
        Assert.assertTrue(containsOnlyOnce(measures, 3));
    }

    @Test
    public void complex() {
        List<String> tags = new ArrayList<String>();
        tags.add("host=local");
        int tagsP = 0;

        List<MemoryTSDB.Measure> values = new ArrayList<MemoryTSDB.Measure>();
        int valueP=0;
        long value = 0;

        String metric = "foo";
        mkMetric(metric);

        long from = now;

        while (now - from < 30*dt) {
            if (from + 2*dt==now) {
                Index index = loader.load();
                index.addIndex(metric, parseTags("cluster=a"));
                tags.add("host=local cluster=a");
            }
            if (from + 10*dt==now) {
                Index index = loader.load();
                index.addIndex(metric, parseTags("cluster=b"));
                tags.add("host=local cluster=b");
            }
            tagsP = (tagsP + 1) % tags.size();
            String submetric = engine.tryMapMetricToSubMetric(metric, now, parseTags(tags.get(tagsP)));
            addMeasure(submetric, now, value, tags.get(tagsP));
            values.add(new MemoryTSDB.Measure(now + dt, value, parseTags(tags.get(tagsP))));

            if (valueP<values.size()) {
                MemoryTSDB.Measure measure = values.get(valueP);
                if (measure.time<=now) {
                    List<MemoryTSDB.Measure> measures = tsdb.query(engine.split(
                        queue(metric, from / 1000, now / 1000, measure.tags)
                    ));
                    Assert.assertTrue(containsOnlyOnce(measures, measure.value));
                    valueP++;
                }
            }

            value++;
            incTime(1000);
        }
    }
}
