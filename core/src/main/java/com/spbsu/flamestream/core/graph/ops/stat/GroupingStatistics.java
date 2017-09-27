package com.spbsu.flamestream.core.graph.ops.stat;

import com.spbsu.flamestream.core.stat.Statistics;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

import static com.spbsu.flamestream.core.stat.Statistics.asMap;

public final class GroupingStatistics implements Statistics {

  private final LongSummaryStatistics replay = new LongSummaryStatistics();
  private final LongSummaryStatistics bucketSize = new LongSummaryStatistics();

  public void recordReplaySize(int replaySize) {
    replay.accept(replaySize);
  }

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
