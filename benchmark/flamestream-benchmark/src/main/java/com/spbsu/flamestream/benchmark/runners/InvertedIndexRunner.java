package com.spbsu.flamestream.benchmark.runners;

import com.spbsu.benchmark.commons.LatencyMeasurer;
import com.spbsu.flamestream.benchmark.EnvironmentRunner;
import com.spbsu.flamestream.core.graph.HashFunction;
import com.spbsu.flamestream.example.FlameStreamExample;
import com.spbsu.flamestream.example.index.model.WikipediaPage;
import com.spbsu.flamestream.example.index.model.WordBase;
import com.spbsu.flamestream.example.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.index.utils.WikipeadiaInput;
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

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class InvertedIndexRunner implements EnvironmentRunner {
  private static final Logger LOG = LoggerFactory.getLogger(InvertedIndexRunner.class);

  @Override
  public void run(Environment environment, Config config) {
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(0, 0);
    final String inputPath = config.hasPath("input-path") ? config.getString("input-path") : null;
    final int limit = config.hasPath("limit") ? config.getInt("limit") : 250;

    final Stream<WikipediaPage> source = (inputPath == null ? WikipeadiaInput.dumpStreamFromResources(
            "wikipedia/national_football_teams_dump.xml") : WikipeadiaInput.dumpStreamFromFile(inputPath)).limit(limit)
            .peek(wikipediaPage -> latencyMeasurer.start(wikipediaPage.id()));

    final int tickLengthInSec = config.getInt("tick-length-sec");
    try (TestEnvironment testEnvironment = new TestEnvironment(environment, 15)) {
      //noinspection RedundantCast,unchecked
      final Consumer<Object> sink = testEnvironment.deploy(
              FlameStreamExample.INVERTED_INDEX.graph(hash -> testEnvironment
                      .wrapInSink(((HashFunction<? super WordBase>) hash), container -> {
                        if (container instanceof WordIndexAdd) {
                          final WordIndexAdd indexAdd = (WordIndexAdd) container;
                          final int docId = IndexItemInLong.pageId(indexAdd.positions()[0]);
                          latencyMeasurer.finish(docId);
                        }
                      })).flattened(), tickLengthInSec, 1, 1); // TODO: 13.11.2017 set proper number of fronts

      final int[] pagesCount = {0};
      final int sleepTimeInMs = config.hasPath("rate") ? config.getInt("rate") : 100;

      source.forEach(wikipediaPage -> {
        sink.accept(wikipediaPage);
        pagesCount[0]++;
        LOG.warn("Page id: {}", wikipediaPage.id());
        try {
          Thread.sleep(sleepTimeInMs);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
      testEnvironment.awaitTicks();

      final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
      LOG.info("Result: {}", stat);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
