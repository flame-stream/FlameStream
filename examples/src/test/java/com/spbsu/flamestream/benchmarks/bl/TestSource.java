package com.spbsu.flamestream.benchmarks.bl;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 25.08.2017
 */
public interface TestSource<T, R> {
  void test(Stream<T> input, Consumer<R> output);

}
