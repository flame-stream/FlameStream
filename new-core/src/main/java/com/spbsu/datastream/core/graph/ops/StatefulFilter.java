package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

import java.util.function.Supplier;

public class StatefulFilter<T, R, S> extends Processor<T, R> {
  private transient S state;

  private final StatefulFunction<T, R, S> function;

  public StatefulFilter(final HashFunction<T> hash,
                        final StatefulFunction<T, R, S> function,
                        final Supplier<S> intiState) {
    super(hash);
    this.state = intiState.get();
    this.function = function;
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    final StatefulFunction.StatefulResult<R, S> result = function.apply((T) item.payload(), state);

    state = result.state();

    handle.push(outPort(), new PayloadDataItem<>(new Meta(item.meta(), this.hashCode()), result.result(), item.rootId()));
    handle.ack(inPort, item);
  }
}

