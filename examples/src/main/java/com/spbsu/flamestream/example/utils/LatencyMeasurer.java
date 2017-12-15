package com.spbsu.flamestream.example.utils;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.TreeMap;

/**
 * User: Artem
 * Date: 15.12.2017
 */
public final class LatencyMeasurer<T> {
  private static final Logger LOG = LoggerFactory.getLogger(LatencyMeasurer.class);

  private final TObjectLongMap<T> starts = new TObjectLongHashMap<>();
  private final Map<T, LongSummaryStatistics> latencies = new TreeMap<>();

  private final long measurePeriod;
  private long delay;

  public LatencyMeasurer(long warmUpDelay, long measurePeriod) {
    this.measurePeriod = measurePeriod;
    this.delay = warmUpDelay;
  }

  public void start(T key) {
    synchronized (this) {
      --delay;
      if (delay > 0) {
        return;
      }
      delay = measurePeriod;

      final long startTs = System.nanoTime();
      starts.put(key, startTs);
      latencies.put(key, new LongSummaryStatistics());
    }
  }

  public void finish(T key) {
    synchronized (this) {
      if (starts.containsKey(key)) {
        final long latency = System.nanoTime() - starts.get(key);
        latencies.computeIfPresent(key, (k, stat) -> {
          stat.accept(latency);
          return stat;
        });
      }
    }
  }

  public long[] latencies() {
    final long[] latenciesDump = latencies.values()
            .stream()
            .map(LongSummaryStatistics::getMax)
            .mapToLong(l -> l)
            .toArray();

    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < latenciesDump.length; i++) {
      stringBuilder.append(latenciesDump[i]);
      if (i != latenciesDump.length - 1) {
        stringBuilder.append(", ");
      }
    }

    LOG.warn("Latencies dump: [{}]", stringBuilder);

    return latenciesDump;
  }
}

