package com.spbsu.benchmark.flink.index.ops;

import com.spbsu.benchmark.flink.index.Result;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TotalOrderEnforcer extends ProcessFunction<Result, Result> {
  private transient NavigableMap<Long, Collection<Result>> buffer;
  private transient Tracing.Tracer inputTracer;
  private transient Tracing.Tracer outputTracer;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    buffer = new TreeMap<>();

    inputTracer = Tracing.TRACING.forEvent("tot-enforcer-receive");
    outputTracer = Tracing.TRACING.forEvent("tot-enforcer-send");
  }

  @Override
  public void processElement(Result value, Context ctx, Collector<Result> out) {
    inputTracer.log(WordIndexAdd.hash(
            value.wordIndexAdd().word(),
            IndexItemInLong.pageId(value.wordIndexAdd().positions()[0])
    ));
    buffer.putIfAbsent(ctx.timestamp(), new ArrayList<>());
    buffer.get(ctx.timestamp()).add(value);
    ctx.timerService().registerEventTimeTimer(ctx.timestamp());
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Result> out) {
    final NavigableMap<Long, Collection<Result>> head = buffer.headMap(timestamp, true);
    head.forEach((ts, value) -> value.stream()
            .peek(result -> outputTracer.log(WordIndexAdd.hash(
                    result.wordIndexAdd().word(),
                    IndexItemInLong.pageId(result.wordIndexAdd().positions()[0])
            )))
            .forEach(out::collect));
    head.clear();
  }
}
