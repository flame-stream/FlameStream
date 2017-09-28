package com.spbsu.flamestream.benchmark.runners;

import com.spbsu.benchmark.LatencyMeasurer;
import com.spbsu.flamestream.benchmark.EnvironmentRunner;
import com.spbsu.flamestream.example.FlameStreamExample;
import com.spbsu.flamestream.example.FlamesStreamTestGraphs;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.inverted_index.utils.WikipeadiaInput;
import com.spbsu.flamestream.runtime.TestEnvironment;
import com.spbsu.flamestream.runtime.environment.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LongSummaryStatistics;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 18.08.2017
 */
@SuppressWarnings("Convert2Lambda")
public class InvertedIndexRunner implements EnvironmentRunner {
  private static final Logger LOG = LoggerFactory.getLogger(InvertedIndexRunner.class);

  @Override
  public void run(Environment environment) {
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(100, 0);
    final Stream<WikipediaPage> source = WikipeadiaInput.dumpStreamFromResources("wikipedia/national_football_teams_dump.xml")
            .peek(wikipediaPage -> latencyMeasurer.start(wikipediaPage.id()));

    try (final TestEnvironment testEnvironment = new TestEnvironment(environment)) {
      testEnvironment.deploy(FlamesStreamTestGraphs.createTheGraph(
              FlameStreamExample.INVERTED_INDEX,
              testEnvironment.availableFronts(),
              testEnvironment.wrapInSink(container -> {
                if (container instanceof WordIndexAdd) {
                  final WordIndexAdd indexAdd = (WordIndexAdd) container;
                  final int docId = IndexItemInLong.pageId(indexAdd.positions()[0]);
                  latencyMeasurer.finish(docId);
                }
              })
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
