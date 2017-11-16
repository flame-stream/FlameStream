package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.common.Statistics;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

import static com.spbsu.flamestream.common.Statistics.asMap;

final class AtomicActorStatistics implements Statistics {

  private final LongSummaryStatistics onAtomic = new LongSummaryStatistics();
  private final LongSummaryStatistics onMinTime = new LongSummaryStatistics();

  void recordOnAtomicMessage(long nanoDuration) {
    onAtomic.accept(nanoDuration);
  }

  void recordOnMinTimeUpdate(long nanoDuration) {
    onMinTime.accept(nanoDuration);
  }

  @Override
  public Map<String, Double> metrics() {
    final Map<String, Double> result = new HashMap<>();
    result.putAll(asMap("onAtomicMessage duration", onAtomic));
    result.putAll(asMap("onMinTime duration", onMinTime));
    return result;
  }

  @Override
  public String toString() {
    return metrics().toString();
  }
}
