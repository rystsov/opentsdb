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

import java.util.Arrays;
import java.util.Map;

/**
 * Helper functions to deal with the row key.
 */
final class RowKey {
    /** For auto create metrics mode, set by --auto-metric flag in TSDMain.  */
    private static final boolean AUTO_METRIC =
            System.getProperty("tsd.core.auto_create_metrics") != null;

    private RowKey() {
        // Can't create instances of this utility class.
    }

    /**
     * Extracts the name of the metric ID contained in a row key.
     *
     * @param tsdb The TSDB to use.
     * @param row  The actual row key.
     * @return The name of the metric.
     */
    static String metricName(final TSDB tsdb, final byte[] row) {
        final byte[] id = Arrays.copyOfRange(row, 0, tsdb.metrics.width());
        return tsdb.metrics.getName(id);
    }


    public static void checkMetricAndTags(String metric, Map<String, String> tags) {
        if (tags.size() <= 0) {
            throw new IllegalArgumentException("Need at least one tags (metric="
                    + metric + ", tags=" + tags + ')');
        } else if (tags.size() > Const.MAX_NUM_TAGS) {
            throw new IllegalArgumentException("Too many tags: " + tags.size()
                    + " maximum allowed: " + Const.MAX_NUM_TAGS + ", tags: " + tags);
        }

        Tags.validateString("metric name", metric);
        for (final Map.Entry<String, String> tag : tags.entrySet()) {
            Tags.validateString("tag name", tag.getKey());
            Tags.validateString("tag value", tag.getValue());
        }
    }

    public static byte[] rowKeyTemplate(TSDB tsdb, String metric, Map<String, String> tags) {
        final short metric_width = tsdb.metrics.width();
        final short tag_name_width = tsdb.tag_names.width();
        final short tag_value_width = tsdb.tag_values.width();
        final short num_tags = (short) tags.size();

        int row_size = (metric_width + Const.TIMESTAMP_BYTES
                + tag_name_width * num_tags
                + tag_value_width * num_tags);
        final byte[] row = new byte[row_size];

        short pos = 0;

        copyInRowKey(row, pos, (AUTO_METRIC ? tsdb.metrics.getOrCreateId(metric)
                : tsdb.metrics.getId(metric)));
        pos += metric_width;

        pos += Const.TIMESTAMP_BYTES;

        for (final byte[] tag : Tags.resolveOrCreateAll(tsdb, tags)) {
            copyInRowKey(row, pos, tag);
            pos += tag.length;
        }
        return row;
    }

    /**
       * Copies the specified byte array at the specified offset in the row key.
       * @param row The row key into which to copy the bytes.
       * @param offset The offset in the row key to start writing at.
       * @param bytes The bytes to copy.
       */
    private static void copyInRowKey(final byte[] row, final short offset, final byte[] bytes) {
        System.arraycopy(bytes, 0, row, offset, bytes.length);
    }
}
