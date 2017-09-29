package com.spbsu.benchmark.commons;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

/**
 * User: Artem
 * Date: 17.08.2017
 */
public class LatencyMeasurer<T> {
  private static final Logger LOG = LoggerFactory.getLogger(LatencyMeasurer.class);

  private final TObjectLongMap<T> starts = new TObjectLongHashMap<>();
  private final Map<T, LongSummaryStatistics> latencies = new HashMap<>();

  private final long measurePeriod;
  private long delay;

  public LatencyMeasurer(long warmUpDelay, long measurePeriod) {
    this.measurePeriod = measurePeriod;
    this.delay = warmUpDelay;
  }

  public void start(T key) {
    if (--delay > 0)
      return;
    delay = measurePeriod;

    final long startTs = System.nanoTime();
    starts.put(key, startTs);
    latencies.put(key, new LongSummaryStatistics());
  }

  public void finish(T key) {
    if (starts.containsKey(key)) {
      final long latency = System.nanoTime() - starts.get(key);
      latencies.computeIfPresent(key, (k, stat) -> {
        stat.accept(latency);
        return stat;
      });
    }
  }

  public long[] latencies() {
    latencies.values().forEach(stat ->
            LOG.info("Latencies distribution for article: {}", stat)
    );
    return latencies.values().stream().map(LongSummaryStatistics::getMax)
            .mapToLong(l -> l).toArray();
  }
}
