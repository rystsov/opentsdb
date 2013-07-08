package net.opentsdb.core.index;

import net.opentsdb.core.TsdbQueryDto;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

/**
 * @author rystsov
 * @date 7/8/13
 */
public class Regressions extends TestBase {
    @Before
    public void init() {
        super.init();
    }

    @Test
    public void testPutInPast() {
        String metric = "foo";
        mkMetric(metric);

        long from = now;
        incTime(dt);

        Index index = loader.load();
        index.addIndex(metric, parseTags("cluster=a"));
        incTime(dt);

        HashMap<String, String> tags = parseTags("cluster=a host=local");
        String submetric = engine.tryMapMetricToSubMetric(metric, from, tags);
        Assert.assertEquals(metric, submetric);
        addMeasure(submetric, from, 1, "cluster=a host=local");
        incTime(dt);

        List<TsdbQueryDto> queries = engine.split(queue(metric, from / 1000, (from + dt/2) / 1000, "cluster=a"));
        Assert.assertEquals(1, queries.size());
        List<MemoryTSDB.Measure> measures = tsdb.query(queries);
        Assert.assertEquals(1, measures.size());
        Assert.assertTrue(containsOnlyOnce(measures, 1));
    }
}
