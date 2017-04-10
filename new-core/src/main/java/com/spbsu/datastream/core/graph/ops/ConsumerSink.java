package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Sink;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.function.Consumer;

public final class ConsumerSink<T> extends Sink<T> {
  private final Consumer<T> consumer;

  public ConsumerSink(final Consumer<T> consumer,
                      final HashFunction<? super T> hash,
                      final HashFunction<? super T> initHash) {
    super(hash, initHash);
    this.consumer = consumer;
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    //noinspection unchecked
    consumer.accept((T) item.payload());
    ack(item, handler);
  }
}
