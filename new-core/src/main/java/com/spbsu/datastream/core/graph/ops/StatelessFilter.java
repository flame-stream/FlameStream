package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public final class StatelessFilter<T, R> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final OutPort outPort = new OutPort();

  private final Function<T, R> function;

  public StatelessFilter(final Function<T, R> function, final HashFunction<? super T> hash) {
    super();
    this.inPort = new InPort(hash);
    this.function = function;
  }

  public Function<T, R> function() {
    return function;
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    @SuppressWarnings("unchecked")
    final R res = function.apply((T) item.payload());

    handler.push(this.outPort(), new PayloadDataItem<>(new Meta(item.meta(), this.incrementLocalTimeAndGet()), res));
    this.ack(item, handler);
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
    final List<OutPort> outPorts = new ArrayList<>();

    outPorts.add(outPort);
    outPorts.add(this.ackPort());

    return Collections.unmodifiableList(outPorts);
  }
}

