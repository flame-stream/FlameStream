package com.spbsu.flamestream.example.benchmark;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 03.01.2018
 */
public interface BenchValidator<T> extends Consumer<T> {
  int inputLimit();

  int expectedOutputSize();

  void stop();
}
