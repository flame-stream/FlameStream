package com.spbsu.datastream.benchmarks.bl.wordcount;

import com.spbsu.datastream.benchmarks.ClusterRunner;
import com.spbsu.datastream.benchmarks.measure.LatencyMeasurer;
import com.spbsu.datastream.benchmarks.measure.LatencyMeasurerDelegate;
import com.spbsu.datastream.core.Cluster;
import com.spbsu.datastream.core.TestStand;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.wordcount.WordCountTest;
import com.spbsu.datastream.core.wordcount.WordCounter;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public final class WordCountRunner implements ClusterRunner {

  @Override
  public void run(Cluster cluster) throws InterruptedException {
    try (TestStand stand = new TestStand(cluster)) {
      final LatencyMeasurer<WordCounter> latencyMeasurer = new LatencyMeasurer<>(new LatencyMeasurerDelegate<WordCounter>() {
        @Override
        public void onStart(WordCounter key) {
          System.out.println("Measuring at " + key.word() + ':' + key.count());
        }

        @Override
        public void onFinish(WordCounter key, long latency) {
          System.out.println("Measured at " + key.word() + ':' + key.count() + ' ' + latency + " nanoseconds");
        }
      }, 1000 * 10, 1000 * 10);
      final TheGraph graph = WordCountTest.wordCountGraph(cluster.fronts(), stand.wrap(o -> latencyMeasurer.finish((WordCounter) o)));
      stand.deploy(graph, 1, TimeUnit.HOURS);

      final TObjectIntMap<String> expected = new TObjectIntHashMap<>();
      final Pattern pattern = Pattern.compile("\\s");
      final Stream<String> input = Stream.generate(() -> new Random().ints(1000, 0, 1000)
              .mapToObj(num -> "word" + num).collect(joining(" ")))
              .limit(10000)
              .peek(text -> Arrays.stream(pattern.split(text)).collect(toMap(Function.identity(), o -> 1, Integer::sum)).forEach((k, v) -> {
                expected.adjustOrPutValue(k, v, v);
                latencyMeasurer.start(new WordCounter(k, expected.get(k)));
              }));

      final Consumer<Object> frontConsumer = stand.randomFrontConsumer(1);
      input.forEach(text -> {
        try {
          frontConsumer.accept(text);
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
      stand.waitTick(1, TimeUnit.HOURS);
    }
  }

}
