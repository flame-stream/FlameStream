package com.spbsu.benchmark.flink;

import com.spbsu.benchmark.commons.LatencyMeasurer;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexRemove;
import com.spbsu.flamestream.example.inverted_index.model.WordPagePositions;
import com.spbsu.flamestream.example.inverted_index.ops.InvertedIndexState;
import com.spbsu.flamestream.example.inverted_index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.inverted_index.utils.WikipediaPageIterator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public class InvertedIndexStream implements FlinkStream<WikipediaPage, InvertedIndexStream.Output> {

  public static void main(String[] args) throws Exception {

    final String hostname;
    final int port;
    final String delimeter;
    final int warmUpDelay;
    final int measurePeriod;
    try {
      final ParameterTool params = ParameterTool.fromArgs(args);
      hostname = params.has("hostname") ? params.get("hostname") : "localhost";
      port = params.getInt("port");
      delimeter = params.get("port");
      warmUpDelay = params.has("warmUpDelay") ? params.getInt("warmUpDelay") : 0;
      measurePeriod = params.has("measurePeriod") ? params.getInt("measurePeriod") : 0;
    } catch (Exception e) {
      System.out.println("Invalid parameters");
      return;
    }

    final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(warmUpDelay, measurePeriod);
    final DataStream<WikipediaPage> source = environment.socketTextStream(hostname, port, delimeter).map(value -> {
      final WikipediaPageIterator pageIterator = new WikipediaPageIterator(
              new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8.name()))
      );
      final WikipediaPage page = pageIterator.next();
      latencyMeasurer.start(page.id());
      return page;
    });

    final FlinkStream<WikipediaPage, InvertedIndexStream.Output> flinkStream = new InvertedIndexStream();
    flinkStream.stream(source).addSink(value -> {
      final WordIndexAdd wordIndexAdd = value.wordIndexAdd();
      final int docId = IndexItemInLong.pageId(wordIndexAdd.positions()[0]);
      latencyMeasurer.finish(docId);
    });

    environment.execute();
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
