package com.spbsu.datastream.core.stat;

import java.util.HashMap;
import java.util.Map;

public final class GroupingStatistics implements Statistics {

  private long cumulativeReplay = 0;
  private long replaySamples = 0;

  public void recordReplaySize(int replaySize) {
    cumulativeReplay += replaySize;
    replaySamples++;
  }

  @Override
  public Map<String, Double> metrics() {
    final Map<String, Double> result = new HashMap<>();
    result.put("Avegage replay size", (double) cumulativeReplay / replaySamples);
    return result;
  }

  @Override
  public String toString() {
    return metrics().toString();
  }
}
