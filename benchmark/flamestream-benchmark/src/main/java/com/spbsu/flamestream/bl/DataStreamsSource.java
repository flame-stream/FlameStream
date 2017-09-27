package com.spbsu.flamestream.example.bl;

import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 25.08.2017
 */
public abstract class DataStreamsSource<T> implements TestSource<T, Object> {
  protected final int fronts;
  protected final int workers;
  protected final int tickLength;

  protected DataStreamsSource(int fronts, int workers, int tickLength) {
    this.fronts = fronts;
    this.workers = workers;
    this.tickLength = tickLength;
  }

  @Override
  public abstract void test(Stream<T> input, Consumer<Object> output);
}
