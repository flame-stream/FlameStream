package com.spbsu.datastream.core.graph;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Created by marnikitta on 2/7/17.
 */
public class ConsumerSink<T> extends Sink {
  private final Consumer<T> consumer;

  public ConsumerSink(final Consumer<T> consumer) {
    this.consumer = consumer;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final ConsumerSink<?> that = (ConsumerSink<?>) o;
    return Objects.equals(consumer, that.consumer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumer);
  }
}
