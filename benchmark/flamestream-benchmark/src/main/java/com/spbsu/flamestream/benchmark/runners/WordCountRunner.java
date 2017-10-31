package com.spbsu.flamestream.benchmark.runners;

import com.spbsu.benchmark.commons.LatencyMeasurer;
import com.spbsu.flamestream.benchmark.EnvironmentRunner;
import com.spbsu.flamestream.example.FlameStreamExample;
import com.spbsu.flamestream.example.wordcount.model.WordCounter;
import com.spbsu.flamestream.runtime.TestEnvironment;
import com.spbsu.flamestream.runtime.environment.Environment;
import com.typesafe.config.Config;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LongSummaryStatistics;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public final class WordCountRunner implements EnvironmentRunner {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountRunner.class);

  @Override
  public void run(Environment environment, Config config) {
    final LatencyMeasurer<WordCounter> latencyMeasurer = new LatencyMeasurer<>(1000 * 10, 1000 * 10);
    final TObjectIntMap<String> expected = new TObjectIntHashMap<>();
    final Stream<String> input = input().peek(text -> {
      final Pattern pattern = Pattern.compile("\\s");
      Arrays.stream(pattern.split(text)).collect(toMap(Function.identity(), o -> 1, Integer::sum)).forEach((k, v) -> {
        expected.adjustOrPutValue(k, v, v);
        latencyMeasurer.start(new WordCounter(k, expected.get(k)));
      });
    });

    try (TestEnvironment testEnvironment = new TestEnvironment(environment, 1)) {
      //noinspection RedundantCast,unchecked
      testEnvironment.deploy(
              testEnvironment.withFusedFronts(FlameStreamExample.WORD_COUNT.graph(hash -> testEnvironment.wrapInSink(
                      (ToIntFunction<? super WordCounter>) hash,
                      o -> latencyMeasurer.finish((WordCounter) o)
              ))),
              60,
              1
      );

      final Consumer<Object> sink = testEnvironment.randomFrontConsumer(1);
      input.forEach(s -> {
        sink.accept(s);
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
      testEnvironment.awaitTick(20);

      final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
      LOG.info("Result: {}", stat);
    }
  }

  private static Stream<String> input() {
    return Stream.generate(() -> new Random().ints(1000, 0, 1000).mapToObj(num -> "word" + num).collect(joining(" ")))
            .limit(500);
  }
}
