package com.spbsu.benchmark.flink.index.ops;

import com.spbsu.benchmark.flink.index.Result;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * User: Artem
 * Date: 04.01.2018
 */
public class SimpleWindow implements AllWindowFunction<Result, Result, TimeWindow> {
  @Override
  public void apply(TimeWindow window, Iterable<Result> values, Collector<Result> out) throws Exception {
    values.forEach(out::collect);
  }
}
