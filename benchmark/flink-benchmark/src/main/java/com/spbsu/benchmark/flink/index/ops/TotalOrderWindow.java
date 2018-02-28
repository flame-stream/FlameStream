package com.spbsu.benchmark.flink.index.ops;

import com.spbsu.benchmark.flink.index.Result;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * User: Artem
 * Date: 05.01.2018
 */
public class TotalOrderWindow implements AllWindowFunction<Result, Result, TimeWindow> {
  @Override
  public void apply(TimeWindow timeWindow, Iterable<Result> iterable, Collector<Result> collector) throws Exception {
    iterable.forEach(collector::collect);
  }
}
