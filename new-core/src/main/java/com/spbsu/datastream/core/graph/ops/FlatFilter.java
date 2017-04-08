package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.function.Function;
import java.util.stream.Stream;

public final class FlatFilter<T, R> extends Processor<T, R> {
  private final Function<T, Stream<R>> function;

  public FlatFilter(final Function<T, Stream<R>> function, final HashFunction<T> hash) {
    super(hash);
    this.function = function;
  }

  public FlatFilter(final Function<T, Stream<R>> function) {
    super();
    this.function = function;
  }

  public Function<T, Stream<R>> function() {
    return function;
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    @SuppressWarnings("unchecked")
    final Stream<R> res = function.apply((T) item.payload());
    res.forEach(r -> {
      handler.push(outPort(), new PayloadDataItem<>(handler.copyAndAppendLocal(item.meta()), r));
    });

    handler.ack(inPort, item);
  }
}
