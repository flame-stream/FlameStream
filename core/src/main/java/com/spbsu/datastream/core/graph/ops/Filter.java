package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

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
    @SuppressWarnings("unchecked") final boolean ok = this.predicate.test((T) item.payload());

    if (ok) {
      final DataItem<?> result = new PayloadDataItem<>(item.meta().advanced(this.incrementLocalTimeAndGet()), item.payload());

      handler.push(this.outPort(), result);
    } else {
      // TODO: 5/9/17 DELIVER NULL
    }
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

