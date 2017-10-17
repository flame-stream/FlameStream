package com.spbsu.benchmark.commons;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.stream.Collectors;

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

  public synchronized void start(T key) {
    if (--delay > 0)
      return;
    delay = measurePeriod;

    final long startTs = System.nanoTime();
    starts.put(key, startTs);
    latencies.put(key, new LongSummaryStatistics());
  }

  public synchronized void finish(T key) {
    if (starts.containsKey(key)) {
      final long latency = System.nanoTime() - starts.get(key);
      latencies.computeIfPresent(key, (k, stat) -> {
        stat.accept(latency);
        return stat;
      });
    }
  }

  public long[] latencies() {
    final List<Long> sizes = latencies.values().stream().map(stat -> stat.getCount()).collect(Collectors.toList());
    System.out.println("SIZES: " + sizes);
    latencies.values().forEach(stat ->
            LOG.warn("Latencies distribution for article: {}", stat)
    );

    final long[] latenciesDump = latencies.values().stream().map(LongSummaryStatistics::getMax)
            .mapToLong(l -> l).toArray();
    final StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < latenciesDump.length; i++) {
      stringBuilder.append(latenciesDump[i]);
      if (i != (latenciesDump.length - 1)) {
        stringBuilder.append(", ");
      }
    }
    LOG.warn("Latencies dump: [{}]", stringBuilder.toString());

    return latenciesDump;
  }
}
