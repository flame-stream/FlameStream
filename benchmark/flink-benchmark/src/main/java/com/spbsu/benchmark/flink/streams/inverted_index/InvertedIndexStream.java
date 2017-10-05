package com.spbsu.benchmark.flink.streams.inverted_index;

import com.spbsu.benchmark.flink.FlinkStream;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexRemove;
import com.spbsu.flamestream.example.inverted_index.model.WordPagePositions;
import com.spbsu.flamestream.example.inverted_index.ops.InvertedIndexState;
import com.spbsu.flamestream.example.inverted_index.utils.IndexItemInLong;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public class InvertedIndexStream implements FlinkStream<WikipediaPage, InvertedIndexFold> {

  @Override
  public DataStream<InvertedIndexFold> stream(DataStream<WikipediaPage> source) {
    //noinspection deprecation
    return source.flatMap(new WikipediaPageToWordPositions())
            .keyBy(0)
            //fold is deprecated but there is no alternative in the current version
            .fold(new InvertedIndexFold(new InvertedIndexState(), null, null), new IndexResult());
  }

  private static class WikipediaPageToWordPositions implements FlatMapFunction<WikipediaPage, Tuple2<String, long[]>> {
    @Override
    public void flatMap(WikipediaPage value, Collector<Tuple2<String, long[]>> out) throws Exception {
      final com.spbsu.flamestream.example.inverted_index.ops.WikipediaPageToWordPositions filter = new com.spbsu.flamestream.example.inverted_index.ops.WikipediaPageToWordPositions();
      final Stream<WordPagePositions> result = filter.apply(value);
      result.forEach(v -> out.collect(new Tuple2<>(v.word(), v.positions())));
    }
  }

  //see comment above about fold
  @SuppressWarnings("deprecation")
  private static class IndexResult implements FoldFunction<Tuple2<String, long[]>, InvertedIndexFold> {
    @Override
    public InvertedIndexFold fold(InvertedIndexFold accumulator, Tuple2<String, long[]> value) throws Exception {
      final long prevValue = accumulator.state().updateOrInsert(value.f1);
      final WordIndexAdd wordIndexAdd = new WordIndexAdd(value.f0, value.f1);
      WordIndexRemove wordIndexRemove = null;
      if (prevValue != InvertedIndexState.PREV_VALUE_NOT_FOUND) {
        wordIndexRemove = new WordIndexRemove(value.f0, IndexItemInLong.setRange(prevValue, 0), IndexItemInLong.range(prevValue));
      }
      return new InvertedIndexFold(accumulator.state(), wordIndexAdd, wordIndexRemove);
    }
  }
}
