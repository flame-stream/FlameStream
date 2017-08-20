package com.spbsu.datastream.benchmarks.bl.wordcount;

import com.spbsu.datastream.benchmarks.measure.LatencyMeasurer;
import com.spbsu.datastream.benchmarks.ClusterRunner;
import com.spbsu.datastream.core.Cluster;
import com.spbsu.datastream.core.TestStand;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.wordcount.WordCountTest;
import com.spbsu.datastream.core.wordcount.WordCounter;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public final class WordCountRunner implements ClusterRunner {
  private static final Pattern PATTERN = Pattern.compile("\\s");
  private static final String[] WORDS = IntStream.range(0, 1000)
          .mapToObj(num -> "word" + num).toArray(String[]::new);

  @Override
  public void run(Cluster cluster) throws InterruptedException {
    try (TestStand stand = new TestStand(cluster)) {
      final LatencyMeasurer<WordCounter> latencyMeasurer = new LatencyMeasurer<>(new WordCounterLatencyDelegate(), 1000 * 10, 1000 * 10);
      final TheGraph graph = WordCountTest.wordCountGraph(cluster.fronts(), stand.wrap(o -> latencyMeasurer.finish((WordCounter) o)));
      final Consumer<Object> frontConsumer = stand.randomFrontConsumer(1);

      stand.deploy(graph, 1, TimeUnit.HOURS);

      final Stream<String> input = Stream.generate(() -> text(1000)).limit(10000)
              .peek(t -> Arrays.stream(PATTERN.split(t)).forEach(s -> latencyMeasurer.start(new WordCounter(s, 0))));

      input.forEach(text -> {
        try {
          frontConsumer.accept(text);
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      stand.waitTick(1, TimeUnit.HOURS);
      latencyMeasurer.stopMeasure();
    }
  }

  private String text(int size) {
    return new Random().ints(size, 0, WORDS.length).distinct()
            .mapToObj(i -> WORDS[i]).collect(joining(" "));
  }
}
