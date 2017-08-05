package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToIntFunction;

public final class StatelessMap<T, R> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final OutPort outPort = new OutPort();

  private final Function<? super T, ? extends R> function;

  public StatelessMap(Function<? super T, ? extends R> function, ToIntFunction<? super T> hash) {
    this.inPort = new InPort(hash);
    this.function = function;
  }

  public Function<? super T, ? extends R> function() {
    return this.function;
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handler) {
    @SuppressWarnings("unchecked") final R res = this.function.apply((T) item.payload());

    final DataItem<R> result = new PayloadDataItem<>(item.meta().advanced(this.incrementLocalTimeAndGet()), res);

    handler.push(this.outPort(), result);
  }

  public InPort inPort() {
    return this.inPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(this.inPort);
  }

  public OutPort outPort() {
    return this.outPort;
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(this.outPort);
  }
}

