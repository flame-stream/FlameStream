package com.spbsu.datastream.core.stat;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

import static com.spbsu.datastream.core.stat.Statistics.asMap;

public final class AtomicActorStatistics implements Statistics {
  private final LongSummaryStatistics onAtomic = new LongSummaryStatistics();

  private final TLongList onAtomicDist = new TLongArrayList();
  public void recordOnAtomicMessage(long nanoDuration) {
    onAtomicDist.add(nanoDuration);
    //onAtomic.accept(nanoDuration);
  }

  private final LongSummaryStatistics onMinTime = new LongSummaryStatistics();

  public void recordOnMinTimeUpdate(long nanoDuration) {
    onMinTime.accept(nanoDuration);
  }

  @Override
  public Map<String, Double> metrics() {
    final Map<String, Double> result = new HashMap<>();
    result.putAll(asMap("onAtomicMessage duration", onAtomic));
    result.putAll(asMap("Average onMinTime duration", onMinTime));
    return result;
  }

  @Override
  public String toString() {
    return "ON ATOMIC DIST: " + onAtomicDist;
    //return metrics().toString();
  }
}
