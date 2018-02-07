package com.spbsu.benchmark.flink.index.ops;

import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordPagePositions;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;
import java.util.stream.Stream;

public class WikipediaPageToWordPositions extends RichFlatMapFunction<WikipediaPage, Tuple2<String, long[]>> {
  private transient Tracing.Tracer inputTracer;
  private transient Tracing.Tracer outputTracer;

  @Override
  public void open(Configuration parameters) throws Exception {
    inputTracer = Tracing.TRACING.forEvent("flatmap-receive", 1000, 1);
    outputTracer = Tracing.TRACING.forEvent("flatmap-send");
  }

  @Override
  public void flatMap(WikipediaPage value, Collector<Tuple2<String, long[]>> out) {
    final com.spbsu.flamestream.example.bl.index.ops.WikipediaPageToWordPositions filter =
            new com.spbsu.flamestream.example.bl.index.ops.WikipediaPageToWordPositions(inputTracer, outputTracer);
    final Stream<WordPagePositions> result = filter.apply(value);
    //noinspection Convert2Lambda
    result.forEach(new Consumer<WordPagePositions>() {
      @Override
      public void accept(WordPagePositions v) {
        out.collect(new Tuple2<>(v.word(), v.positions()));
      }
    });
  }
}
