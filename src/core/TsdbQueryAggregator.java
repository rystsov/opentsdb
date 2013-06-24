// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import org.hbase.async.HBaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.hbase.async.Bytes.ByteMap;

/**
 * Non-synchronized implementation of {@link net.opentsdb.core.Query}.
 */
public final class TsdbQueryAggregator {
    //
    private static final Logger LOG = LoggerFactory.getLogger(TsdbQueryAggregator.class);
    private static final byte[] empty = new byte[0];

    /**
     * The TSDB we belong to.
     */
    private final TSDB tsdb;
    final short value_width;

    /**
     * Constructor.
     */
    public TsdbQueryAggregator(final TSDB tsdb) {
        this.tsdb = tsdb;
        this.value_width = tsdb.tag_values.width();
    }

    public static DataPoints[] execute(TSDB tsdb, TsdbQueryDto query) {
        return new TsdbQueryAggregator(tsdb).run(query);
    }

    // Maps group value IDs to the SpanGroup for those values.  Say we've
    // been asked to group by two things: foo=* bar=* Then the keys in this
    // map will contain all the value IDs combinations we've seen.  If the
    // name IDs for `foo' and `bar' are respectively [0, 0, 7] and [0, 0, 2]
    // then we'll have group_bys=[[0, 0, 2], [0, 0, 7]] (notice it's sorted
    // by ID, so bar is first) and say we find foo=LOL bar=OMG as well as
    // foo=LOL bar=WTF and that the IDs of the tag values are:
    //   LOL=[0, 0, 1]  OMG=[0, 0, 4]  WTF=[0, 0, 3]
    // then the map will have two keys:
    //   - one for the LOL-OMG combination: [0, 0, 1, 0, 0, 4] and,
    //   - one for the LOL-WTF combination: [0, 0, 1, 0, 0, 3].
    final ByteMap<SpanGroup> groups = new ByteMap<SpanGroup>();

    private DataPoints[] run(TsdbQueryDto query) throws HBaseException {
        TsdbQueryLoader loader = new TsdbQueryLoader(tsdb);
        List<TsdbQueryDto> splitted = tsdb.splitIfFederated(query);
        for (TsdbQueryDto subQuery : splitted) {
            groupByAndAggregate(subQuery, loader.findSpans(subQuery));
        }
        return groups.values().toArray(new SpanGroup[groups.size()]);
    }


    /**
     * Creates the {@link net.opentsdb.core.SpanGroup}s to form the final results of this query.
     *
     * @param spans The {@link net.opentsdb.core.Span}s found for this query (...).
     *              Can be {@code null}, in which case the array returned will be empty.
     * @return A possibly empty array of {@link net.opentsdb.core.SpanGroup}s built according to
     *         any 'GROUP BY' formulated in this query.
     */
    private void groupByAndAggregate(TsdbQueryDto query, final TreeMap<byte[], Span> spans) {
        if (spans == null || spans.size() <= 0) return;
        final byte[] key = query.group_bys == null ? empty : new byte[query.group_bys.size() * value_width];

        for (final Map.Entry<byte[], Span> entry : spans.entrySet()) {
            final byte[] row = entry.getKey();

            if (!fillKey(key, query, row)) {
                LOG.error("WTF?  Dropping span for row " + Arrays.toString(row) + " as it had no matching tag from the requested groups," + " which is unexpected.  Query=" + query);
                continue;
            }

            SpanGroup thegroup = groups.get(key);
            if (thegroup == null) {
                thegroup = newGroup(query);
                groups.put(Arrays.copyOf(key, key.length), thegroup);
            }
            thegroup.add(entry.getValue());
        }
    }

    private boolean fillKey(byte[] group, TsdbQueryDto query, byte[] row) {
        if (query.group_bys == null) return true;
        byte[] value_id = null;
        int i = 0;
        // TODO(tsuna): The following loop has a quadratic behavior.  We can
        // make it much better since both the row key and group_bys are sorted.
        for (final byte[] tag_id : query.group_bys) {
            value_id = Tags.getValueId(tsdb, row, tag_id);
            if (value_id == null) {
                break;
            }
            System.arraycopy(value_id, 0, group, i, value_width);
            i += value_width;
        }
        if (value_id == null) {
            return false;
        }
        return true;
    }

    private SpanGroup newGroup(TsdbQueryDto query) {
        return new SpanGroup(tsdb, query.getScanStartTime(), getScanEndTime(query),
                null, query.rate, query.aggregator,
                query.sample_interval, query.downsampler);
    }

    /**
     * Returns the UNIX timestamp at which we must stop scanning.
     */
    private static long getScanEndTime(TsdbQueryDto query) {
        // For the end_time, we have a different problem.  For instance if our
        // end_time = ... 12:30:00, we'll stop scanning when we get to 12:40, but
        // once again we wanna try to look ahead one more row, so to avoid this
        // problem we always add 1 second to the end_time.  Only when the end_time
        // is of the form HH:59:59 then we will scan ahead an extra row, but once
        // again that doesn't really matter.
        // Additionally, in case our sample_interval is large, we need to look
        // even further before/after, so use that too.
        return
                (query.end_time == null ? System.currentTimeMillis() / 1000 : query.end_time) +
                        Const.MAX_TIMESPAN + 1 + query.sample_interval;
    }

}
