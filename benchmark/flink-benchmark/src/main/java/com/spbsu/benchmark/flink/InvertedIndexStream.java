package com.spbsu.benchmark.flink;

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
public class InvertedIndexStream implements FlinkStream<WikipediaPage, InvertedIndexStream.Output> {

  public static void main(String[] args) {
    // TODO: 05.10.2017 parse arguments

    /*final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(100, 0);*/

    // TODO: 05.10.2017 parse socket
    final DataStream<WikipediaPage> source = null;// = environment.socketTextStream()
    final FlinkStream<WikipediaPage, InvertedIndexStream.Output> flinkStream = new InvertedIndexStream();

    flinkStream.stream(source).addSink(value -> {
      // TODO: 05.10.2017 measure latency
    });
  }

  @Override
  public DataStream<Output> stream(DataStream<WikipediaPage> source) {
    //noinspection deprecation
    return source.flatMap(new WikipediaPageToWordPositions())
            .keyBy(0)
            //fold is deprecated but there is no alternative in the current version
            .fold(new Output(new InvertedIndexState(), null, null), new IndexResult());
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
  private static class IndexResult implements FoldFunction<Tuple2<String, long[]>, Output> {
    @Override
    public Output fold(Output accumulator, Tuple2<String, long[]> value) throws Exception {
      final long prevValue = accumulator.state().updateOrInsert(value.f1);
      final WordIndexAdd wordIndexAdd = new WordIndexAdd(value.f0, value.f1);
      WordIndexRemove wordIndexRemove = null;
      if (prevValue != InvertedIndexState.PREV_VALUE_NOT_FOUND) {
        wordIndexRemove = new WordIndexRemove(value.f0, IndexItemInLong.setRange(prevValue, 0), IndexItemInLong.range(prevValue));
      }
      return new Output(accumulator.state(), wordIndexAdd, wordIndexRemove);
    }
  }

  static class Output {
    private final InvertedIndexState state;
    private final WordIndexAdd wordIndexAdd;
    private final WordIndexRemove wordIndexRemove;

    Output(InvertedIndexState state, WordIndexAdd wordIndexAdd, WordIndexRemove wordIndexRemove) {
      this.state = state;
      this.wordIndexAdd = wordIndexAdd;
      this.wordIndexRemove = wordIndexRemove;
    }

    InvertedIndexState state() {
      return state;
    }

    WordIndexAdd wordIndexAdd() {
      return wordIndexAdd;
    }

    WordIndexRemove wordIndexRemove() {
      return wordIndexRemove;
    }
  }
}
