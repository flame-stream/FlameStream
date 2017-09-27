package com.spbsu.flamestream.example;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public interface ExampleChecker<T, R> {
  Stream<T> input();

  void check(Stream<R> output);
}
