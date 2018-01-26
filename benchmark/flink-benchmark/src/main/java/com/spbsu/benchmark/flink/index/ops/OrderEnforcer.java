package com.spbsu.benchmark.flink.index.ops;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;

public class OrderEnforcer extends ProcessFunction<Tuple2<String, long[]>, Tuple2<String, long[]>> {
  private transient NavigableMap<Long, Collection<Tuple2<String, long[]>>> buffer;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    buffer = new TreeMap<>();
  }

  @Override
  public void processElement(Tuple2<String, long[]> value,
                             Context ctx,
                             Collector<Tuple2<String, long[]>> out) {
    buffer.putIfAbsent(ctx.timestamp(), new ArrayList<>());
    buffer.get(ctx.timestamp()).add(value);
    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, long[]>> out) {
    final NavigableMap<Long, Collection<Tuple2<String, long[]>>> head = buffer.headMap(timestamp, true);
    head.forEach((ts, value) -> value.forEach(out::collect));
    head.clear();
  }
}
