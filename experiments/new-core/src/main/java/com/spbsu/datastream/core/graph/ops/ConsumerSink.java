package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Sink;
import com.spbsu.datastream.core.materializer.GraphStageLogic;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class ConsumerSink<T> extends Sink<T> {
  private final Consumer<DataItem<T>> consumer;

  public ConsumerSink(final Consumer<DataItem<T>> consumer) {
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

  @Override
  public GraphStageLogic<T, Void> logic() {
    return new GraphStageLogic<T, Void>() {
      public void onPush(final InPort inPort, final DataItem<T> item) {
        consumer.accept(item);
      }
    };
  }

  @Override
  public Graph deepCopy() {
    return new ConsumerSink<>(consumer);
  }
}
