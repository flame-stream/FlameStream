package com.spbsu.datastream.benchmarks.bl.inverted_index;

import com.spbsu.datastream.benchmarks.bl.TestSource;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WikipediaPage;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 25.08.2017
 */
public class FlinkIndexSource implements TestSource<WikipediaPage, Object> {
  private final int bufferTimeout;

  FlinkIndexSource(int bufferTimeout) {
    this.bufferTimeout = bufferTimeout;
  }

  @Override
  public void test(Stream<WikipediaPage> input, Consumer<Object> output) {
    InvertedIndexFlinkRunner.test(input, output, bufferTimeout);
  }
}
