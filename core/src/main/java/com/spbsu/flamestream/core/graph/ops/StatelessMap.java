package com.spbsu.flamestream.core.graph.ops;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;

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
    return function;
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handler) {
    @SuppressWarnings("unchecked") final R res = function.apply((T) item.payload());

    final DataItem<R> result = new PayloadDataItem<>(item.meta().advanced(incrementLocalTimeAndGet()), res);

    handler.push(outPort(), result);
    handler.ack(result.ack(), result.meta().globalTime());
  }

  public InPort inPort() {
    return inPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(inPort);
  }

  public OutPort outPort() {
    return outPort;
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
  }
}

