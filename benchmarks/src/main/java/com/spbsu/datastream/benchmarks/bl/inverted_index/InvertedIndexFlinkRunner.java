package com.spbsu.datastream.benchmarks.bl.inverted_index;

import com.spbsu.datastream.benchmarks.ClusterRunner;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WikipediaPage;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WordIndexAdd;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WordIndexRemove;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WordPagePositions;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ops.InvertedIndexState;
import com.spbsu.datastream.benchmarks.bl.inverted_index.utils.IndexLongUtil;
import com.spbsu.datastream.benchmarks.bl.inverted_index.utils.InputUtils;
import com.spbsu.datastream.benchmarks.measure.LatencyMeasurer;
import com.spbsu.datastream.benchmarks.measure.LatencyMeasurerDelegate;
import com.spbsu.datastream.core.Cluster;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 21.08.2017
 */
public class InvertedIndexFlinkRunner implements ClusterRunner {
  //TODO: make iterator serializable and use org.apache.flink.streaming.api.functions.source.FromIteratorFunction
  private static Iterator<WikipediaPage> iterator;
  //TODO: make consumer serializable
  private static Consumer<Object> consumer;

  @Override
  public void run(Cluster cluster) throws InterruptedException {
    //TODO: refactor ClusterRunner for flink
    try {
      final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(new LatencyMeasurerDelegate<Integer>() {
        @Override
        public void onStart(Integer key) {
        }

        @Override
        public void onFinish(Integer key, long latency) {
        }
      }, 100, 0);
      final Stream<WikipediaPage> source = InputUtils.dumpStreamFromResources("wikipedia/national_football_teams_dump.xml")
              .peek(wikipediaPage -> latencyMeasurer.start(wikipediaPage.id()));
      test(source, container -> {
        if (container instanceof WordIndexAdd) {
          final WordIndexAdd indexAdd = (WordIndexAdd) container;
          final int docId = IndexLongUtil.pageId(indexAdd.positions()[0]);
          latencyMeasurer.finish(docId);
        }
      }, 0);

      final long[] latencies = latencyMeasurer.latencies();
      System.out.println("Size: " + latencies.length);
      Arrays.stream(latencies).forEach(System.out::println);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  static void test(Stream<WikipediaPage> source, Consumer<Object> output, int bufferTimeout) {
    try {
      iterator = source.iterator();
      consumer = output;

      final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
      env.setBufferTimeout(bufferTimeout);
      //noinspection deprecation
      env.addSource(new WikiPagesSource())
              .flatMap(new WikipediaPageToWordPositions())
              .keyBy(0)
              //fold is deprecated but there is no alternative in the current version
              .fold(new Tuple3<>(new InvertedIndexState(), null, null), new IndexResult())
              .addSink(value -> {
                consumer.accept(value.f1);
                if (value.f2 != null)
                  consumer.accept(value.f2);
              });
      env.execute();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodError ignore) {
      //akka now does not have shutdown method
    }
  }

  private static class WikiPagesSource implements SourceFunction<WikipediaPage> {
    private boolean running = true;

    @Override
    public void run(SourceContext<WikipediaPage> ctx) throws Exception {
      while (running) {
        if (iterator.hasNext()) {
          ctx.collect(iterator.next());
          ctx.emitWatermark(new Watermark(System.nanoTime()));
        } else {
          running = false;
          ctx.close();
        }
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }

  private static class WikipediaPageToWordPositions implements FlatMapFunction<WikipediaPage, Tuple2<String, long[]>> {
    @Override
    public void flatMap(WikipediaPage value, Collector<Tuple2<String, long[]>> out) throws Exception {
      final com.spbsu.datastream.benchmarks.bl.inverted_index.ops.WikipediaPageToWordPositions filter = new com.spbsu.datastream.benchmarks.bl.inverted_index.ops.WikipediaPageToWordPositions();
      final Stream<WordPagePositions> result = filter.apply(value);
      result.forEach(v -> out.collect(new Tuple2<>(v.word(), v.positions())));
    }
  }

  //see comment above about fold
  @SuppressWarnings("deprecation")
  private static class IndexResult implements FoldFunction<Tuple2<String, long[]>, Tuple3<InvertedIndexState, WordIndexAdd, WordIndexRemove>> {
    @Override
    public Tuple3<InvertedIndexState, WordIndexAdd, WordIndexRemove> fold(Tuple3<InvertedIndexState, WordIndexAdd, WordIndexRemove> accumulator, Tuple2<String, long[]> value) throws Exception {
      final long prevValue = accumulator.f0.updateOrInsert(value.f1);
      final WordIndexAdd wordIndexAdd = new WordIndexAdd(value.f0, value.f1);
      WordIndexRemove wordIndexRemove = null;
      if (prevValue != InvertedIndexState.PREV_VALUE_NOT_FOUND) {
        wordIndexRemove = new WordIndexRemove(value.f0, IndexLongUtil.setRange(prevValue, 0), IndexLongUtil.range(prevValue));
      }
      accumulator.setField(wordIndexAdd, 1);
      accumulator.setField(wordIndexRemove, 2);
      return accumulator;
    }
  }
}
