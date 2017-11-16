package com.spbsu.flamestream.runtime.ack;

import com.spbsu.flamestream.common.Statistics;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

import static com.spbsu.flamestream.common.Statistics.asMap;

public final class AckerStatistics implements Statistics {
  private final LongSummaryStatistics normalAck = new LongSummaryStatistics();
  private final LongSummaryStatistics releasingAck = new LongSummaryStatistics();

  void recordNormalAck(long ts) {
    normalAck.accept(ts);
  }

  void recordReleasingAck(long ts) {
    releasingAck.accept(ts);
  }

  @Override
  public Map<String, Double> metrics() {
    final Map<String, Double> result = new HashMap<>();
    result.putAll(asMap("Normal ack duration", normalAck));
    result.putAll(asMap("Releasing ack duration", releasingAck));
    return result;
  }

  @Override
  public String toString() {
    return metrics().toString();
  }
}
