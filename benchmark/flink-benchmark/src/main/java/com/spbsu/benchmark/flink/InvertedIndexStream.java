package com.spbsu.benchmark.flink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.flamestream.example.index.model.WikipediaPage;
import com.spbsu.flamestream.example.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.index.model.WordPagePositions;
import com.spbsu.flamestream.example.index.ops.InvertedIndexState;
import com.spbsu.flamestream.example.index.utils.IndexItemInLong;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public class InvertedIndexStream implements FlinkStream<WikipediaPage, InvertedIndexStream.Result> {
  public DataStream<Result> stream(DataStream<WikipediaPage> source, int parallelism) {
    return source.flatMap(new WikipediaPageToWordPositions())
            .setParallelism(parallelism)
            .keyBy(0)
            .map(new RichIndexFunction())
            .setParallelism(parallelism);
  }

  @Override
  public DataStream<Result> stream(DataStream<WikipediaPage> source) {
    return source.flatMap(new WikipediaPageToWordPositions()).keyBy(0).map(new RichIndexFunction());
  }

  private static class WikipediaPageToWordPositions implements FlatMapFunction<WikipediaPage, Tuple2<String, long[]>> {
    @Override
    public void flatMap(WikipediaPage value, Collector<Tuple2<String, long[]>> out) {
      final com.spbsu.flamestream.example.index.ops.WikipediaPageToWordPositions filter =
              new com.spbsu.flamestream.example.index.ops.WikipediaPageToWordPositions();
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

  private static class RichIndexFunction extends RichMapFunction<Tuple2<String, long[]>, Result> {

    private transient ValueState<InvertedIndexState> state = null;

    @Override
    public void open(Configuration parameters) {
      final ValueStateDescriptor<InvertedIndexState> descriptor = new ValueStateDescriptor<>(
              "index",
              InvertedIndexState.class
      );

      state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public Result map(Tuple2<String, long[]> value) throws Exception {
      final InvertedIndexState currentStat;
      if (state.value() == null) {
        currentStat = new InvertedIndexState();
      } else {
        currentStat = state.value();
      }

      final long prevValue = currentStat.updateOrInsert(value.f1);
      final WordIndexAdd wordIndexAdd = new WordIndexAdd(value.f0, value.f1);
      WordIndexRemove wordIndexRemove = null;
      if (prevValue != InvertedIndexState.PREV_VALUE_NOT_FOUND) {
        wordIndexRemove = new WordIndexRemove(
                value.f0,
                IndexItemInLong.setRange(prevValue, 0),
                IndexItemInLong.range(prevValue)
        );
      }
      state.update(currentStat);
      return new Result(wordIndexAdd, wordIndexRemove);
    }
  }

  public static class Result {
    private final WordIndexAdd wordIndexAdd;
    private final WordIndexRemove wordIndexRemove;

    @JsonCreator
    Result(@JsonProperty("word_index_add") WordIndexAdd wordIndexAdd,
            @JsonProperty("word_index_remove") WordIndexRemove wordIndexRemove) {
      this.wordIndexAdd = wordIndexAdd;
      this.wordIndexRemove = wordIndexRemove;
    }

    @JsonProperty("word_index_add")
    WordIndexAdd wordIndexAdd() {
      return wordIndexAdd;
    }

    @JsonProperty("word_index_remove")
    WordIndexRemove wordIndexRemove() {
      return wordIndexRemove;
    }

    @Override
    public String toString() {
      return "Result{" + "wordIndexAdd=" + wordIndexAdd + ", wordIndexRemove=" + wordIndexRemove + '}';
    }
  }
}
