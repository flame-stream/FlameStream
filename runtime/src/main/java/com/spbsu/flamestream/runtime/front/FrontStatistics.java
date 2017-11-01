package com.spbsu.flamestream.runtime.front;

import com.spbsu.flamestream.core.stat.Statistics;

import java.util.LongSummaryStatistics;
import java.util.Map;

import static com.spbsu.flamestream.core.stat.Statistics.asMap;

/**
 * User: Artem
 * Date: 20.10.2017
 */
public class FrontStatistics implements Statistics {
  private final LongSummaryStatistics nanosBetweenPings = new LongSummaryStatistics();
  private long prevPing = -1;

  void ping() {
    if (prevPing != -1) {
      nanosBetweenPings.accept(System.nanoTime() - prevPing);
    }
    prevPing = System.nanoTime();
  }

  @Override
  public Map<String, Double> metrics() {
    return asMap("Nanos between pings", nanosBetweenPings);
  }

  @Override
  public String toString() {
    return metrics().toString();
  }
}
