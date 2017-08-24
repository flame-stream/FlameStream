package com.spbsu.datastream.benchmarks.bl.inverted_index;

import com.spbsu.datastream.benchmarks.bl.TestSource;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WikipediaPage;
import com.spbsu.datastream.core.LocalCluster;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 25.08.2017
 */
public class DataStreamsIndexSource implements TestSource<WikipediaPage, Object> {
  private final int fronts;
  private final int workers;
  private final int tickLength;

  DataStreamsIndexSource(int fronts, int workers, int tickLength) {
    this.fronts = fronts;
    this.workers = workers;
    this.tickLength = tickLength;
  }

  @Override
  public void test(Stream<WikipediaPage> input, Consumer<Object> output) {
    try {
      try (final LocalCluster cluster = new LocalCluster(workers, fronts)) {
        InvertedIndexRunner.test(cluster, input, output, tickLength);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
