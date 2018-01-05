package com.spbsu.benchmark.flink.index.ops;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * User: Artem
 * Date: 04.01.2018
 */
public class LocalOrderWindow implements AllWindowFunction<Tuple2<String, long[]>, Tuple2<String, long[]>, TimeWindow> {
  @Override
  public void apply(
          TimeWindow timeWindow,
          Iterable<Tuple2<String, long[]>> iterable,
          Collector<Tuple2<String, long[]>> collector
  ) throws Exception {
    iterable.forEach(collector::collect);
  }
}
