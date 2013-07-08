package net.opentsdb.core;

import com.stumbleupon.async.Deferred;
import org.hbase.async.Bytes;
import org.hbase.async.PutRequest;

import java.util.*;

/**
 * @author rystsov
 * @date 7/8/13
 */
public class IncomingTimeShardedDataPoints implements WritableDataPointsLight {
    private String metric = null;
    private Map<String, String> tags = null;
    private Boolean batchornot = null;

    private Map<String, IncomingDataPoints> shards = new HashMap<String, IncomingDataPoints>();
    private final TSDB tsdb;

    public IncomingTimeShardedDataPoints(TSDB tsdb) {
        this.tsdb = tsdb;
    }

    @Override
    public Deferred<Object> addPoint(long timestamp, long value) {
        return getShard(timestamp).addPoint(timestamp, value);
    }

    @Override
    public Deferred<Object> addPoint(long timestamp, float value) {
        return getShard(timestamp).addPoint(timestamp, value);
    }

    @Override
    public void setSeries(String metric, Map<String, String> tags) {
        if (metric==null) throw new IllegalArgumentException("metric can't be null");
        if (tags==null) throw new IllegalArgumentException("tags can't be null");
        if (this.metric!=null) throw new IllegalArgumentException("metric can't be set twice");
        this.metric = metric;
        this.tags = tags;
    }

    @Override
    public void setBatchImport(boolean batchornot) {
        if (this.batchornot!=null) throw new IllegalArgumentException("batchornot can't be set twice");
        this.batchornot = batchornot;
    }

    private IncomingDataPoints getShard(long timestamp) {
        String submetric = tsdb.tryMapMetricToSubMetric(metric, timestamp*1000, tags);
        if (!shards.containsKey(submetric)) {
            IncomingDataPoints income = new IncomingDataPoints(tsdb);
            income.setSeries(submetric, tags);
            income.setBatchImport(batchornot==null ? false : batchornot);
            shards.put(submetric, income);
        }
        return shards.get(submetric);
    }


/**
 * Receives new data points and stores them in HBase.
 */
final private static class IncomingDataPoints {
  /** For how long to buffer edits when doing batch imports (in ms).  */
  private static final short DEFAULT_BATCH_IMPORT_BUFFER_INTERVAL = 5000;

  /** The {@code TSDB} instance we belong to. */
  private final TSDB tsdb;

  /**
   * The row key.
   * 3 bytes for the metric name, 4 bytes for the base timestamp, 6 bytes per
   * tag (3 for the name, 3 for the value).
   */
  private byte[] row;

  /**
   * Qualifiers for individual data points.
   * The last Const.FLAG_BITS bits are used to store flags (the type of the
   * data point - integer or floating point - and the size of the data point
   * in bytes).  The remaining MSBs store a delta in seconds from the base
   * timestamp stored in the row key.
   */
  private short[] qualifiers;

  /** Each value in the row. */
  private long[] values;

  /** Number of data points in this row. */
  private short size;

  /** Are we doing a batch import? */
  private boolean batch_import;

  /**
   * Constructor.
   * @param tsdb The TSDB we belong to.
   */
  public IncomingDataPoints(final TSDB tsdb) {
    this.tsdb = tsdb;
    this.qualifiers = new short[3];
    this.values = new long[3];
  }

  public void setSeries(String metric, final Map<String, String> tags) {
    RowKey.checkMetricAndTags(metric, tags);
    row = RowKey.rowKeyTemplate(tsdb, metric, tags);
    size = 0;
  }

  public Deferred<Object> addPoint(final long timestamp, final long value) {
    final short flags = 0x7;  // An int stored on 8 bytes.
    return addPointInternal(timestamp, Bytes.fromLong(value), flags);
  }

  public Deferred<Object> addPoint(final long timestamp, final float value) {
    if (Float.isNaN(value) || Float.isInfinite(value)) {
      throw new IllegalArgumentException("value is NaN or Infinite: " + value
                                         + " for timestamp=" + timestamp);
    }
    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
    return addPointInternal(timestamp,
                            Bytes.fromInt(Float.floatToRawIntBits(value)),
                            flags);
  }

  public void setBatchImport(final boolean batchornot) {
    if (batch_import == batchornot) {
      return;
    }
    final long current_interval = tsdb.client.getFlushInterval();
    if (batchornot) {
      batch_import = true;
      // If we already were given a larger interval, don't override it.
      if (DEFAULT_BATCH_IMPORT_BUFFER_INTERVAL > current_interval) {
        setBufferingTime(DEFAULT_BATCH_IMPORT_BUFFER_INTERVAL);
      }
    } else {
      batch_import = false;
      // If we're using the default batch import buffer interval,
      // revert back to 0.
      if (current_interval == DEFAULT_BATCH_IMPORT_BUFFER_INTERVAL) {
        setBufferingTime((short) 0);
      }
    }
  }


  /**
   * Updates the base time in the row key.
   * @param timestamp The timestamp from which to derive the new base time.
   * @return The updated base time.
   */
  private long updateBaseTime(final long timestamp) {
    // We force the starting timestamp to be on a MAX_TIMESPAN boundary
    // so that all TSDs create rows with the same base time.  Otherwise
    // we'd need to coordinate TSDs to avoid creating rows that cover
    // overlapping time periods.
    final long base_time = timestamp - (timestamp % Const.MAX_TIMESPAN);
    // Clone the row key since we're going to change it.  We must clone it
    // because the HBase client may still hold a reference to it in its
    // internal datastructures.
    row = Arrays.copyOf(row, row.length);
    Bytes.setInt(row, (int) base_time, tsdb.metrics.width());
    tsdb.scheduleForCompaction(row, (int) base_time);
    return base_time;
  }

  /**
   * Implements {@link #addPoint} by storing a value with a specific flag.
   * @param timestamp The timestamp to associate with the value.
   * @param value The value to store.
   * @param flags Flags to store in the qualifier (size and type of the data
   * point).
   * @return A deferred object that indicates the completion of the request.
   */
  private Deferred<Object> addPointInternal(final long timestamp, final byte[] value,
                                            final short flags) {
    // This particular code path only expects integers on 8 bytes or floating
    // point values on 4 bytes.
    assert value.length == 8 || value.length == 4 : Bytes.pretty(value);
    if (row == null) {
      throw new IllegalStateException("setSeries() never called!");
    }
    if ((timestamp & 0xFFFFFFFF00000000L) != 0) {
      // => timestamp < 0 || timestamp > Integer.MAX_VALUE
      throw new IllegalArgumentException((timestamp < 0 ? "negative " : "bad")
          + " timestamp=" + timestamp
          + " when trying to add value=" + Arrays.toString(value) + " to " + this);
    }

    long base_time;
    if (size > 0) {
      base_time = baseTime();
      final long last_ts = base_time + (delta(qualifiers[size - 1]));
      if (timestamp <= last_ts) {
        throw new IllegalArgumentException("New timestamp=" + timestamp
            + " is less than previous=" + last_ts
            + " when trying to add value=" + Arrays.toString(value)
            + " to " + this);
      } else if (timestamp - base_time >= Const.MAX_TIMESPAN) {
        // Need to start a new row as we've exceeded Const.MAX_TIMESPAN.
        base_time = updateBaseTime(timestamp);
        size = 0;
        //LOG.info("Starting a new row @ " + this);
      }
    } else {
      // This is the first data point, let's record the starting timestamp.
      base_time = updateBaseTime(timestamp);
      Bytes.setInt(row, (int) base_time, tsdb.metrics.width());
    }

    if (values.length == size) {
      grow();
    }

    // Java is so stupid with its auto-promotion of int to float.
    final short qualifier = (short) ((timestamp - base_time) << Const.FLAG_BITS
                                     | flags);
    qualifiers[size] = qualifier;
    values[size] = (value.length == 8
                    ? Bytes.getLong(value)
                    : Bytes.getInt(value) & 0x00000000FFFFFFFFL);
    size++;

    final PutRequest point = new PutRequest(tsdb.table, row, TSDB.FAMILY,
                                            Bytes.fromShort(qualifier),
                                            value);
    // TODO(tsuna): The following timing is rather useless.  First of all,
    // the histogram never resets, so it tends to converge to a certain
    // distribution and never changes.  What we really want is a moving
    // histogram so we can see how the latency distribution varies over time.
    // The other problem is that the Histogram class isn't thread-safe and
    // here we access it from a callback that runs in an unknown thread, so
    // we might miss some increments.  So let's comment this out until we
    // have a proper thread-safe moving histogram.
    //final long start_put = System.nanoTime();
    //final Callback<Object, Object> cb = new Callback<Object, Object>() {
    //  public Object call(final Object arg) {
    //    putlatency.add((int) ((System.nanoTime() - start_put) / 1000000));
    //    return arg;
    //  }
    //  public String toString() {
    //    return "time put request";
    //  }
    //};

    // TODO(tsuna): Add an errback to handle some error cases here.
    point.setDurable(!batch_import);
    return tsdb.client.put(point)/*.addBoth(cb)*/;
  }

  private void grow() {
    // We can't have more than 1 value per second, so MAX_TIMESPAN values.
    final int new_size = Math.min(size * 2, Const.MAX_TIMESPAN);
    if (new_size == size) {
      throw new AssertionError("Can't grow " + this + " larger than " + size);
    }
    values = Arrays.copyOf(values, new_size);
    qualifiers = Arrays.copyOf(qualifiers, new_size);
  }

  /** Extracts the base timestamp from the row key. */
  private long baseTime() {
    return Bytes.getUnsignedInt(row, tsdb.metrics.width());
  }

  private void setBufferingTime(final short time) {
    if (time < 0) {
      throw new IllegalArgumentException("negative time: " + time);
    }
    tsdb.client.setFlushInterval(time);
  }

  /** Returns a human readable string representation of the object. */
  public String toString() {
    // The argument passed to StringBuilder is a pretty good estimate of the
    // length of the final string based on the row key and number of elements.
    final String metric = metricName();
    final StringBuilder buf = new StringBuilder(80 + metric.length()
                                                + row.length * 4 + size * 16);
    final long base_time = baseTime();
    buf.append("IncomingDataPoints(")
       .append(row == null ? "<null>" : Arrays.toString(row))
       .append(" (metric=")
       .append(metric)
       .append("), base_time=")
       .append(base_time)
       .append(" (")
       .append(base_time > 0 ? new Date(base_time * 1000) : "no date")
       .append("), [");
    for (short i = 0; i < size; i++) {
      buf.append('+').append(delta(qualifiers[i]));
      if (isInteger(i)) {
        buf.append(":long(").append(longValue(i));
      } else {
        buf.append(":float(").append(doubleValue(i));
      }
      buf.append(')');
      if (i != size - 1) {
        buf.append(", ");
      }
    }
    buf.append("])");
    return buf.toString();
  }

    private String metricName() {
      if (row == null) {
        throw new IllegalStateException("setSeries never called before!");
      }
      final byte[] id = Arrays.copyOfRange(row, 0, tsdb.metrics.width());
      return tsdb.metrics.getName(id);
    }

    /** @throws IndexOutOfBoundsException if {@code i} is out of bounds. */
    private void checkIndex(final int i) {
      if (i > size) {
        throw new IndexOutOfBoundsException("index " + i + " > " + size
            + " for this=" + this);
      }
      if (i < 0) {
        throw new IndexOutOfBoundsException("negative index " + i
            + " for this=" + this);
      }
    }

    private static short delta(final short qualifier) {
      return (short) ((qualifier & 0xFFFF) >>> Const.FLAG_BITS);
    }

    private long timestamp(final int i) {
      checkIndex(i);
      return baseTime() + (delta(qualifiers[i]) & 0xFFFF);
    }

    private boolean isInteger(final int i) {
      checkIndex(i);
      return (qualifiers[i] & Const.FLAG_FLOAT) == 0x0;
    }

    private long longValue(final int i) {
      // Don't call checkIndex(i) because isInteger(i) already calls it.
      if (isInteger(i)) {
        return values[i];
      }
      throw new ClassCastException("value #" + i + " is not a long in " + this);
    }

    private double doubleValue(final int i) {
      // Don't call checkIndex(i) because isInteger(i) already calls it.
      if (!isInteger(i)) {
        return Float.intBitsToFloat((int) values[i]);
      }
      throw new ClassCastException("value #" + i + " is not a float in " + this);
    }
}


}
