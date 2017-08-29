package com.spbsu.datastream.core.stat;

import java.util.HashMap;
import java.util.Map;

public final class AtomicActorStatistics implements Statistics {
  private long cumulativeOnAtomic = 0;
  private int onAtomicSamples = 0;

  public void recordOnAtomicMessage(long nanoDuration) {
    cumulativeOnAtomic += nanoDuration;
    onAtomicSamples++;
  }

  private long cumulativeOnMinTime = 0;
  private long onMinTimeSamples = 0;

  public void recordOnMinTimeUpdate(long nanoDuration) {
    cumulativeOnMinTime += nanoDuration;
    onMinTimeSamples++;
  }

  @Override
  public Map<String, Double> metrics() {
    final Map<String, Double> result = new HashMap<>();
    result.put("Average onAtomicMessage duration", (double) cumulativeOnAtomic / onAtomicSamples);
    result.put("Average onMinTime duration", (double) cumulativeOnMinTime / onMinTimeSamples);
    return result;
  }

  @Override
  public String toString() {
    return metrics().toString();
  }
}
