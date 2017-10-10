package com.spbsu.flamestream.benchmark.runners;

import com.spbsu.benchmark.commons.LatencyMeasurer;
import com.spbsu.flamestream.benchmark.EnvironmentRunner;
import com.spbsu.flamestream.example.FlameStreamExample;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.inverted_index.utils.WikipeadiaInput;
import com.spbsu.flamestream.runtime.TestEnvironment;
import com.spbsu.flamestream.runtime.environment.Environment;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LongSummaryStatistics;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * User: Artem
 * Date: 18.08.2017
 */
@SuppressWarnings("Convert2Lambda")
public class InvertedIndexRunner implements EnvironmentRunner {
  private static final Logger LOG = LoggerFactory.getLogger(InvertedIndexRunner.class);

  @Override
  public void run(Environment environment, Config config) {
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(100, 0);
    final String inputPath = config.hasPath("input-path") ? config.getString("input-path") : null;
    final Stream<WikipediaPage> source = (inputPath == null ?
            WikipeadiaInput.dumpStreamFromResources("wikipedia/national_football_teams_dump.xml")
            : WikipeadiaInput.dumpStreamFromFile(inputPath)
    ).peek(wikipediaPage -> latencyMeasurer.start(wikipediaPage.id()));

    try (TestEnvironment testEnvironment = new TestEnvironment(environment, MILLISECONDS.toNanos(1))) {
      //noinspection RedundantCast,unchecked
      testEnvironment.deploy(testEnvironment.withFusedFronts(
              FlameStreamExample.INVERTED_INDEX.graph(
                      hash -> testEnvironment.wrapInSink(((ToIntFunction<? super WordIndexAdd>) hash), container -> {
                        if (container instanceof WordIndexAdd) {
                          final WordIndexAdd indexAdd = (WordIndexAdd) container;
                          final int docId = IndexItemInLong.pageId(indexAdd.positions()[0]);
                          latencyMeasurer.finish(docId);
                        }
                      })
              )
      ), 40, 1);

      final Consumer<Object> sink = testEnvironment.randomFrontConsumer(1);
      source.forEach(wikipediaPage -> {
        sink.accept(wikipediaPage);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException();
        }
      });
      testEnvironment.awaitTick(25);

      final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
      LOG.info("Result: {}", stat);
    }
  }
}
