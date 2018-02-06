package com.spbsu.benchmark.flink.index.ops;

import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class OrderEnforcer extends ProcessFunction<Tuple2<String, long[]>, Tuple2<String, long[]>> {
  private transient NavigableMap<Long, Collection<Tuple2<String, long[]>>> buffer;
  private transient Tracing.Tracer inputTracer;
  private transient Tracing.Tracer outputTracer;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    buffer = new TreeMap<>();

    inputTracer = Tracing.TRACING.forEvent("enforcer-receive", 1000, 1);
    outputTracer = Tracing.TRACING.forEvent("enforcer-send");
  }

  @Override
  public void processElement(Tuple2<String, long[]> value,
                             Context ctx,
                             Collector<Tuple2<String, long[]>> out) {
    inputTracer.log(Objects.hash(value.f0, IndexItemInLong.pageId(value.f1[0])));

    buffer.putIfAbsent(ctx.timestamp(), new ArrayList<>());
    buffer.get(ctx.timestamp()).add(value);
    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, long[]>> out) {
    final NavigableMap<Long, Collection<Tuple2<String, long[]>>> head = buffer.headMap(timestamp, true);
    head.forEach((ts, value) -> value.stream()
            .peek(tuple -> outputTracer.log(Objects.hash(tuple.f0, IndexItemInLong.pageId(tuple.f1[0]))))
            .forEach(out::collect));
    head.clear();
  }

  @Override
  public void close() throws Exception {
    super.close();
    Tracing.TRACING.flush(Paths.get("tmp/enforcer.csv"));
  }
}
