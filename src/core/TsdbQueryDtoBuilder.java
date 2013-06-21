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

import org.hbase.async.Bytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.hbase.async.Bytes.ByteMap;

/**
 * Non-synchronized implementation of {@link net.opentsdb.core.Query}.
 */
public final class TsdbQueryDtoBuilder {
    private final TsdbQueryDto core;
    private final TSDB tsdb;

    public TsdbQueryDtoBuilder(final TSDB tsdb) {
        this.core = new TsdbQueryDto();
        this.tsdb = tsdb;
    }

    public TsdbQueryDto build() {
        if (core.group_bys != null) {
          Collections.sort(core.group_bys, Bytes.MEMCMP);
        }
        core.validate();
        return  core;
    }

    public TsdbQueryDtoBuilder setStartTime(final long timestamp) {
        if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
            throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
        }
        core.start_time = timestamp & 0x00000000FFFFFFFFL;
        return this;
    }


    public TsdbQueryDtoBuilder setEndTime(final long timestamp) {
        if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
            throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
        }
        core.end_time = timestamp;
        return this;
    }

    public TsdbQueryDtoBuilder setMetric(final String metric) {
        core.metric = tsdb.metrics.getId(metric);
        core.metricText = metric;
        return this;
    }

    public TsdbQueryDtoBuilder setAggregator(final Aggregator aggregator) {
        core.aggregator = aggregator;
        return this;
    }

    public TsdbQueryDtoBuilder setRate(final boolean rate) {
        core.rate = rate;
        return this;
    }

    public TsdbQueryDtoBuilder setTags(final Map<String, String> tags) {
        findGroupBys(tsdb, core, tags);
        core.tags = Tags.resolveAll(tsdb, tags);
        core.tagsText = tags;
        return this;
    }

    public void setDownsample(final int interval, final Aggregator downsampler) {
        core.downsampler = downsampler;
        core.sample_interval = interval;
    }


    /**
     * Extracts all the tags we must use to group results.
     * <ul>
     * <li>If a tag has the form {@code name=*} then we'll create one
     * group per value we find for that tag.</li>
     * <li>If a tag has the form {@code name={v1,v2,..,vN}} then we'll
     * create {@code N} groups.</li>
     * </ul>
     * In the both cases above, {@code name} will be stored in the
     * {@code group_bys} attribute.  In the second case specifically,
     * the {@code N} values would be stored in {@code group_by_values},
     * the key in this map being {@code name}.
     *
     * @param tags The tags from which to extract the 'GROUP BY's.
     *             Each tag that represents a 'GROUP BY' will be removed from the map
     *             passed in argument.
     */
    private static void findGroupBys(final TSDB tsdb, final TsdbQueryDto core, final Map<String, String> tags) {
        final Iterator<Map.Entry<String, String>> i = tags.entrySet().iterator();
        while (i.hasNext()) {
            final Map.Entry<String, String> tag = i.next();
            final String tagvalue = tag.getValue();
            if (tagvalue.equals("*")  // 'GROUP BY' with any value.
                    || tagvalue.indexOf('|', 1) >= 0) {  // Multiple possible values.
                if (core.group_bys == null) {
                    core.group_bys = new ArrayList<byte[]>();
                }
                core.group_bys.add(tsdb.tag_names.getId(tag.getKey()));
                i.remove();
                if (tagvalue.charAt(0) == '*') {
                    continue;  // For a 'GROUP BY' with any value, we're done.
                }
                // 'GROUP BY' with specific values.  Need to split the values
                // to group on and store their IDs in group_by_values.
                final String[] values = Tags.splitString(tagvalue, '|');
                if (core.group_by_values == null) {
                    core.group_by_values = new ByteMap<byte[][]>();
                }
                final short value_width = tsdb.tag_values.width();
                final byte[][] value_ids = new byte[values.length][value_width];
                core.group_by_values.put(tsdb.tag_names.getId(tag.getKey()),
                        value_ids);
                for (int j = 0; j < values.length; j++) {
                    final byte[] value_id = tsdb.tag_values.getId(values[j]);
                    System.arraycopy(value_id, 0, value_ids[j], 0, value_width);
                }
            }
        }
    }

}
