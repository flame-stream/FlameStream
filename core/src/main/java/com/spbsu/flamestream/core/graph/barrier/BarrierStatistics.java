package com.spbsu.flamestream.core.graph.barrier;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.stat.Statistics;
import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

import static com.spbsu.flamestream.core.stat.Statistics.*;

/**
 * User: Artem
 * Date: 13.10.2017
 */
public class BarrierStatistics implements Statistics {
  private final TObjectLongMap<GlobalTime> timeMeasure = new TObjectLongHashMap<>();
  private final LongSummaryStatistics duration = new LongSummaryStatistics();

  void enqueue(GlobalTime globalTime) {
    timeMeasure.put(globalTime, System.nanoTime());
  }

  void release(GlobalTime globalTime) {
    final long start = timeMeasure.get(globalTime);
    if (start != Constants.DEFAULT_LONG_NO_ENTRY_VALUE) {
      duration.accept(System.nanoTime() - start);
      timeMeasure.remove(globalTime);
    }
  }

  @Override
  public Map<String, Double> metrics() {
    return asMap("Barrier releasing duration", duration);
  }

  @Override
  public String toString() {
    return metrics().toString();
  }
}
