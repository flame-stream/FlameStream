package com.spbsu.benchmark.flink.index.ops;

import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordPagePositions;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;
import java.util.stream.Stream;

public class WikipediaPageToWordPositions implements FlatMapFunction<WikipediaPage, Tuple2<String, long[]>> {
  @Override
  public void flatMap(WikipediaPage value, Collector<Tuple2<String, long[]>> out) {
    final com.spbsu.flamestream.example.bl.index.ops.WikipediaPageToWordPositions filter =
            new com.spbsu.flamestream.example.bl.index.ops.WikipediaPageToWordPositions();
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
