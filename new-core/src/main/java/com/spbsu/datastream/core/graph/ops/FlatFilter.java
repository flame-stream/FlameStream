package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

import java.util.function.Function;
import java.util.stream.Stream;

public final class FlatFilter<T, R> extends Processor<T, R> {
  // TODO: 3/26/17 LOOK AT ME
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
      handler.push(outPort(), new PayloadDataItem<>(new Meta(item.meta(), this.hashCode()), r, item.rootId()));
    });

    handler.ack(inPort, item);
  }
}
