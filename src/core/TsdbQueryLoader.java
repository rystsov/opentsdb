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

import net.opentsdb.stats.Histogram;
import net.opentsdb.uid.NoSuchUniqueId;
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

public final class TsdbQueryLoader {
    /**
     * Comparator that ignores timestamps in row keys.
     */
    private static final class SpanCmp implements Comparator<byte[]> {

        private final short metric_width;

        public SpanCmp(final short metric_width) {
            this.metric_width = metric_width;
        }

        public int compare(final byte[] a, final byte[] b) {
            final int length = Math.min(a.length, b.length);
            if (a == b) {  // Do this after accessing a.length and b.length
                return 0;    // in order to NPE if either a or b is null.
            }
            int i;
            // First compare the metric ID.
            for (i = 0; i < metric_width; i++) {
                if (a[i] != b[i]) {
                    return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
                }
            }
            // Then skip the timestamp and compare the rest.
            for (i += Const.TIMESTAMP_BYTES; i < length; i++) {
                if (a[i] != b[i]) {
                    return (a[i] & 0xFF) - (b[i] & 0xFF);  // "promote" to unsigned.
                }
            }
            return a.length - b.length;
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(TsdbQueryLoader.class);
//
//  /**
//   * Charset to use with our server-side row-filter.
//   * We use this one because it preserves every possible byte unchanged.
//   */
    private static final Charset CHARSET = Charset.forName("ISO-8859-1");
//
    /**
     * The TSDB we belong to.
     * Sets in constructor only
     */
    private final TSDB tsdb;

    //  /** Constructor. */
    public TsdbQueryLoader(final TSDB tsdb) {
        this.tsdb = tsdb;
    }

    /**
     * Finds all the {@link net.opentsdb.core.Span}s that match this query.
     * This is what actually scans the HBase table and loads the data into
     * {@link net.opentsdb.core.Span}s.
     *
     * @return A map from HBase row key to the {@link net.opentsdb.core.Span} for that row key.
     *         Since a {@link net.opentsdb.core.Span} actually contains multiple HBase rows, the row key
     *         stored in the map has its timestamp zero'ed out.
     * @throws org.hbase.async.HBaseException if there was a problem communicating with HBase to
     *                                        perform the search.
     * @throws IllegalArgumentException       if bad data was retreived from HBase.
     */
    public TreeMap<byte[], Span> findSpans(final TsdbQueryDto query) throws HBaseException {
        final short metric_width = tsdb.metrics.width();
        final TreeMap<byte[], Span> spans =  // The key is a row key from HBase.
                new TreeMap<byte[], Span>(new SpanCmp(metric_width));
        int nrows = 0;
        int hbase_time = 0;  // milliseconds.
        long starttime = System.nanoTime();
        final Scanner scanner = getScanner(tsdb, query);
        try {
            ArrayList<ArrayList<KeyValue>> rows;
            while ((rows = scanner.nextRows().joinUninterruptibly()) != null) {
                hbase_time += (System.nanoTime() - starttime) / 1000000;
                for (final ArrayList<KeyValue> row : rows) {
                    final byte[] key = row.get(0).key();
                    if (Bytes.memcmp(query.metric, key, 0, metric_width) != 0) {
                        throw new IllegalDataException("HBase returned a row that doesn't match"
                                + " our scanner (" + scanner + ")! " + row + " does not start"
                                + " with " + Arrays.toString(query.metric));
                    }
                    Span datapoints = spans.get(key);
                    if (datapoints == null) {
                        datapoints = new Span(tsdb);
                        spans.put(key, datapoints);
                    }
                    final KeyValue compacted = tsdb.compact(row);
                    if (compacted != null) {  // Can be null if we ignored all KVs.
                        datapoints.addRow(compacted);
                        nrows++;
                    }
                    starttime = System.nanoTime();
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Should never be here", e);
        } finally {
            hbase_time += (System.nanoTime() - starttime) / 1000000;
            TSDB.scanlatency.add(hbase_time);
        }
        LOG.info(this + " matched " + nrows + " rows in " + spans.size() + " spans");
        if (nrows == 0) {
            return null;
        }
        return spans;
    }

    /**
     * Creates the {@link org.hbase.async.Scanner} to use for this query.
     */
    public static Scanner getScanner(TSDB tsdb, TsdbQueryDto query) throws HBaseException {
        final short metric_width = tsdb.metrics.width();
        final byte[] start_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
        final byte[] end_row = new byte[metric_width + Const.TIMESTAMP_BYTES];
        // We search at least one row before and one row after the start & end
        // time we've been given as it's quite likely that the exact timestamp
        // we're looking for is in the middle of a row.  Plus, a number of things
        // rely on having a few extra data points before & after the exact start
        // & end dates in order to do proper rate calculation or downsampling near
        // the "edges" of the graph.
        Bytes.setInt(start_row, (int) query.getScanStartTime(), metric_width);
        Bytes.setInt(end_row, (query.end_time == null
                ? -1  // Will scan until the end (0xFFF...).
                : (int) query.getScanEndTime()),
                metric_width);
        System.arraycopy(query.metric, 0, start_row, 0, metric_width);
        System.arraycopy(query.metric, 0, end_row, 0, metric_width);

        final Scanner scanner = tsdb.client.newScanner(tsdb.table);
        scanner.setStartKey(start_row);
        scanner.setStopKey(end_row);
        if (query.tags.size() > 0 || query.group_bys != null) {
            createAndSetFilter(tsdb, query, scanner);
        }
        scanner.setFamily(TSDB.FAMILY);
        return scanner;
    }

    /**
     * Sets the server-side regexp filter on the scanner.
     * In order to find the rows with the relevant tags, we use a
     * server-side filter that matches a regular expression on the row key.
     *
     * @param scanner The scanner on which to add the filter.
     */
    static void createAndSetFilter(final TSDB tsdb, final TsdbQueryDto query, final Scanner scanner) {
        final short name_width = tsdb.tag_names.width();
        final short value_width = tsdb.tag_values.width();
        final short tagsize = (short) (name_width + value_width);
        // Generate a regexp for our tags.  Say we have 2 tags: { 0 0 1 0 0 2 }
        // and { 4 5 6 9 8 7 }, the regexp will be:
        // "^.{7}(?:.{6})*\\Q\000\000\001\000\000\002\\E(?:.{6})*\\Q\004\005\006\011\010\007\\E(?:.{6})*$"
        final StringBuilder buf = new StringBuilder(
                15  // "^.{N}" + "(?:.{M})*" + "$"
                        + ((13 + tagsize) // "(?:.{M})*\\Q" + tagsize bytes + "\\E"
                        * (query.tags.size() + (query.group_bys == null ? 0 : query.group_bys.size() * 3))));
        // In order to avoid re-allocations, reserve a bit more w/ groups ^^^

        // Alright, let's build this regexp.  From the beginning...
        buf.append("(?s)"  // Ensure we use the DOTALL flag.
                + "^.{")
                // ... start by skipping the metric ID and timestamp.
                .append(tsdb.metrics.width() + Const.TIMESTAMP_BYTES)
                .append("}");
        final Iterator<byte[]> tags = query.tags.iterator();
        final Iterator<byte[]> group_bys = (query.group_bys == null
                ? new ArrayList<byte[]>(0).iterator()
                : query.group_bys.iterator());
        byte[] tag = tags.hasNext() ? tags.next() : null;
        byte[] group_by = group_bys.hasNext() ? group_bys.next() : null;
        // Tags and group_bys are already sorted.  We need to put them in the
        // regexp in order by ID, which means we just merge two sorted lists.
        do {
            // Skip any number of tags.
            buf.append("(?:.{").append(tagsize).append("})*\\Q");
            if (isTagNext(query, name_width, tag, group_by)) {
                addId(buf, tag);
                tag = tags.hasNext() ? tags.next() : null;
            } else {  // Add a group_by.
                addId(buf, group_by);
                final byte[][] value_ids = (query.group_by_values == null
                        ? null
                        : query.group_by_values.get(group_by));
                if (value_ids == null) {  // We don't want any specific ID...
                    buf.append(".{").append(value_width).append('}');  // Any value ID.
                } else {  // We want specific IDs.  List them: /(AAA|BBB|CCC|..)/
                    buf.append("(?:");
                    for (final byte[] value_id : value_ids) {
                        buf.append("\\Q");
                        addId(buf, value_id);
                        buf.append('|');
                    }
                    // Replace the pipe of the last iteration.
                    buf.setCharAt(buf.length() - 1, ')');
                }
                group_by = group_bys.hasNext() ? group_bys.next() : null;
            }
        } while (tag != group_by);  // Stop when they both become null.
        // Skip any number of tags before the end.
        buf.append("(?:.{").append(tagsize).append("})*$");
        scanner.setKeyRegexp(buf.toString(), CHARSET);
    }

    /**
     * Helper comparison function to compare tag name IDs.
     *
     * @param name_width Number of bytes used by a tag name ID.
     * @param tag        A tag (array containing a tag name ID and a tag value ID).
     * @param group_by   A tag name ID.
     * @return {@code true} number if {@code tag} should be used next (because
     *         it contains a smaller ID), {@code false} otherwise.
     */
    static private boolean isTagNext(final TsdbQueryDto query,
                              final short name_width,
                              final byte[] tag,
                              final byte[] group_by) {
        if (tag == null) {
            return false;
        } else if (group_by == null) {
            return true;
        }
        final int cmp = Bytes.memcmp(tag, group_by, 0, name_width);
        if (cmp == 0) {
            throw new AssertionError("invariant violation: tag ID "
                    + Arrays.toString(group_by) + " is both in 'tags' and"
                    + " 'group_bys' in " + query);
        }
        return cmp < 0;
    }

    /**
     * Appends the given ID to the given buffer, followed by "\\E".
     */
    private static void addId(final StringBuilder buf, final byte[] id) {
        boolean backslash = false;
        for (final byte b : id) {
            buf.append((char) (b & 0xFF));
            if (b == 'E' && backslash) {  // If we saw a `\' and now we have a `E'.
                // So we just terminated the quoted section because we just added \E
                // to `buf'.  So let's put a litteral \E now and start quoting again.
                buf.append("\\\\E\\Q");
            } else {
                backslash = b == '\\';
            }
        }
        buf.append("\\E");
    }
}
