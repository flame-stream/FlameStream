package com.spbsu.datastream.benchmarks.wordcount;

import com.spbsu.datastream.core.Cluster;
import com.spbsu.datastream.core.LocalCluster;
import com.spbsu.datastream.core.TestStand;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.wordcount.WordCountTest;
import com.sun.corba.se.spi.orbutil.threadpool.ThreadPoolChooser;
import org.jooq.lambda.Unchecked;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

public final class WordCountRunner {
  private static final String[] WORDS = IntStream.range(0, 1000)
          .mapToObj(num -> "word" + num).toArray(String[]::new);

  private final Cluster cluster;

  public WordCountRunner(Cluster cluster) {
    this.cluster = cluster;
  }

  public static void main(String... args) throws InterruptedException {
    new WordCountRunner(new LocalCluster(1, 1)).run();
  }

  public void run() throws InterruptedException {
    try (TestStand stand = new TestStand(cluster)) {
      final LatencyMeasurer latencyMeasurer = new LatencyMeasurer(1000 * 10, 1000 * 10);
      final TheGraph graph = WordCountTest.wordCountGraph(cluster.fronts(), stand.wrap(latencyMeasurer));
      final Consumer<Object> frontConsumer = stand.randomFrontConsumer(1);

      stand.deploy(graph, 1, TimeUnit.HOURS);

      final Stream<String> input = Stream.generate(() -> text(1000)).limit(10000)
              .peek(latencyMeasurer::logNewText);

      input.forEach(text -> {
        try {
          frontConsumer.accept(text);
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      stand.waitTick(1, TimeUnit.HOURS);
    }
  }

  private String text(int size) {
    return new Random().ints(size, 0, WORDS.length)
            .mapToObj(i -> WORDS[i]).collect(joining(" "));
  }
}
