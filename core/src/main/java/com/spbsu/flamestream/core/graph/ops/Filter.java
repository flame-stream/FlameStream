package com.spbsu.flamestream.core.graph.ops;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;

import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

public final class Filter<T> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final OutPort outPort = new OutPort();

  private final Predicate<T> predicate;

  public Filter(Predicate<T> predicate, ToIntFunction<? super T> hash) {
    this.inPort = new InPort(hash);
    this.predicate = predicate;
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handler) {
    @SuppressWarnings("unchecked") final boolean ok = predicate.test((T) item.payload());

    if (ok) {
      final DataItem<?> result = new PayloadDataItem<>(item.meta().advanced(incrementLocalTimeAndGet()), item.payload());

      handler.push(outPort(), result);
      handler.ack(result.ack(), result.meta().globalTime());
    } else {
      // TODO: 5/9/17 DELIVER NULL
    }
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

