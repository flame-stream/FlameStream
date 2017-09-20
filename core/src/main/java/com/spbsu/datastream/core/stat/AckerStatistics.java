package com.spbsu.datastream.core.stat;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

import static com.spbsu.datastream.core.stat.Statistics.*;

public final class AckerStatistics implements Statistics {
  private final LongSummaryStatistics normalAck = new LongSummaryStatistics();
  public void recordNormalAck(long ts) {
    normalAck.accept(ts);
  }

  private final LongSummaryStatistics releasingAck = new LongSummaryStatistics();
  public void recordReleasingAck(long ts) {
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
