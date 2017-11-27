package com.spbsu.flamestream.runtime.utils;

import java.util.HashMap;
import java.util.LongSummaryStatistics;
import java.util.Map;

public interface Statistics {
  static Map<String, Double> asMap(String prefix, LongSummaryStatistics summaryStatistics) {
    final Map<String, Double> result = new HashMap<>();
    result.put(prefix + " count", (double) summaryStatistics.getCount());
    result.put(prefix + " average", summaryStatistics.getAverage());
    result.put(prefix + " min", (double) summaryStatistics.getMin());
    result.put(prefix + " max", (double) summaryStatistics.getMax());
    return result;
  }

  Map<String, Double> metrics();
}
