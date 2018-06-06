package com.spbsu.flamestream.example.benchmark;

import java.util.LongSummaryStatistics;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class LatencyMeasurer {
  private final long start;
  private final LongSummaryStatistics statistics = new LongSummaryStatistics();

  public LatencyMeasurer() {
    this.start = System.nanoTime();
  }

  public void finish() {
    statistics.accept(System.nanoTime() - start);
  }

  public LongSummaryStatistics statistics() {
    return statistics;
  }
}
