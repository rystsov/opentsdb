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

import net.opentsdb.uid.NoSuchUniqueName;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseException;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;

import static org.hbase.async.Bytes.ByteMap;

/**
 * Non-synchronized implementation of {@link Query}.
 */
public final class TsdbQueryDto {
    // (UNIX timestamp in seconds) on 32 bits ("unsigned" int).
    public Long start_time = null;
    // (UNIX timestamp in seconds) on 32 bits ("unsigned" int).
    public Long end_time = null;
    // ID of the metric being looked up.
    public byte[] metric;
    public String metricText;
    /**
     * Tags of the metrics being looked up.
     * Each tag is a byte array holding the ID of both the name and value
     * of the tag.
     * Invariant: an element cannot be both in this array and in group_bys.
     */
    public ArrayList<byte[]> tags;
    public Map<String, String> tagsText;

    /**
     * Tags by which we must group the results.
     * Each element is a tag ID.
     * Invariant: an element cannot be both in this array and in {@code tags}.
     */
    public ArrayList<byte[]> group_bys;

    /**
     * Values we may be grouping on.
     * For certain elements in {@code group_bys}, we may have a specific list of
     * values IDs we're looking for.  Those IDs are stored in this map.  The key
     * is an element of {@code group_bys} (so a tag name ID) and the values are
     * tag value IDs (at least two).
     */
    public ByteMap<byte[][]> group_by_values;

    /**
     * If true, use rate of change instead of actual values.
     */
    public boolean rate = false;

    /**
     * Aggregator function to use.
     */
    public Aggregator aggregator = null;

    /**
     * Downsampling function to use, if any (can be {@code null}).
     * If this is non-null, {@code sample_interval} must be strictly positive.
     * Sets in downsample only
     */
    public Aggregator downsampler = null;

    /**
     * Minimum time interval (in seconds) wanted between each data point.
     * Sets in downsample only
     */
    public int sample_interval = 0;

    public void validate() {
        if (start_time == null) throw new IllegalStateException("start_time must be initialised");
        if (end_time != null) {
            if (start_time >= end_time) {
                throw new IllegalArgumentException(
                        "new start time (" + start_time + ") is greater than or equal to end time: " + end_time
                );
            }
        }
        if (aggregator == null) throw new IllegalStateException("aggregator must be initialised");
        if (metric == null) throw new IllegalStateException("metric must be initialised");
        if (downsampler != null) {
            if (sample_interval <= 0) throw new IllegalArgumentException("interval not > 0: " + sample_interval);
        }
    }

    /**
     * Returns the UNIX timestamp from which we must start scanning.
     */
    public long getScanStartTime() {
        // The reason we look before by `MAX_TIMESPAN * 2' seconds is because of
        // the following.  Let's assume MAX_TIMESPAN = 600 (10 minutes) and the
        // start_time = ... 12:31:00.  If we initialize the scanner to look
        // only 10 minutes before, we'll start scanning at time=12:21, which will
        // give us the row that starts at 12:30 (remember: rows are always aligned
        // on MAX_TIMESPAN boundaries -- so in this example, on 10m boundaries).
        // But we need to start scanning at least 1 row before, so we actually
        // look back by twice MAX_TIMESPAN.  Only when start_time is aligned on a
        // MAX_TIMESPAN boundary then we'll mistakenly scan back by an extra row,
        // but this doesn't really matter.
        // Additionally, in case our sample_interval is large, we need to look
        // even further before/after, so use that too.
        final long ts = this.start_time - Const.MAX_TIMESPAN * 2 - this.sample_interval;
        return ts > 0 ? ts : 0;
    }

    /**
     * Returns the UNIX timestamp at which we must stop scanning.
     */
    public long getScanEndTime() {
        if (this.end_time == null) throw new IllegalStateException("end_time is not initialized");
        // For the end_time, we have a different problem.  For instance if our
        // end_time = ... 12:30:00, we'll stop scanning when we get to 12:40, but
        // once again we wanna try to look ahead one more row, so to avoid this
        // problem we always add 1 second to the end_time.  Only when the end_time
        // is of the form HH:59:59 then we will scan ahead an extra row, but once
        // again that doesn't really matter.
        // Additionally, in case our sample_interval is large, we need to look
        // even further before/after, so use that too.
        return this.end_time + Const.MAX_TIMESPAN + 1 + this.sample_interval;
    }
}
