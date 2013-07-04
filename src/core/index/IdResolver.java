package net.opentsdb.core.index;

/**
 * @author rystsov
 * @date 7/4/13
 */
public interface IdResolver {
    byte[] resolveMetric(String metric);
}
