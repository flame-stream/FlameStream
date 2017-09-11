package com.spbsu.datastream.core.stat;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

import static com.spbsu.datastream.core.stat.Statistics.asMap;

public final class GroupingStatistics implements Statistics {

  private final LongSummaryStatistics replay = new LongSummaryStatistics();

  public void recordReplaySize(int replaySize) {
    replay.accept(replaySize);
  }

  private final LongSummaryStatistics bucketSize = new LongSummaryStatistics();

  public void recordBucketSize(long size) {
    bucketSize.accept(size);
  }

  @Override
  public Map<String, Double> metrics() {
    final Map<String, Double> result = new HashMap<>();
    result.putAll(asMap("Replay", replay));
    result.putAll(asMap("Bucket size", bucketSize));
    return result;
  }

  @Override
  public String toString() {
    return metrics().toString();
  }
}
