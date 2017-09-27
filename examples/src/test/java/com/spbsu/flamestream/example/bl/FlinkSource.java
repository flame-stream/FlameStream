package com.spbsu.flamestream.example.bl;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 25.08.2017
 */
public abstract class FlinkSource<T> implements TestSource<T, Object> {
  protected final int bufferTimeout;

  protected FlinkSource(int bufferTimeout) {
    this.bufferTimeout = bufferTimeout;
  }

  @Override
  public abstract void test(Stream<T> input, Consumer<Object> output);
}
