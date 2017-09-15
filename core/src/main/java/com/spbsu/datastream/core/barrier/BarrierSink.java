package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.Collections;
import java.util.List;

public abstract class BarrierSink<T> extends AbstractAtomicGraph {
  private final InPort inPort;

  private final BarrierCollector collector = new LinearCollector();

  public BarrierSink() {
    this.inPort = new InPort(PreSinkMetaElement.HASH_FUNCTION);
  }

  @Override
  public final void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    this.collector.enqueue(item);
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
    this.collector.update(globalTime);
    this.collector.release(di -> this.consume(((DataItem<PreSinkMetaElement<T>>) di).payload().payload()));
  }

  @Override
  public final void onCommit(AtomicHandle handle) {
    if (!this.collector.isEmpty()) {
      throw new IllegalStateException("Barrier should be empty");
    }
  }

  protected abstract void consume(T payload);

  public final InPort inPort() {
    return this.inPort;
  }

  @Override
  public final List<InPort> inPorts() {
    return Collections.singletonList(this.inPort);
  }

  @Override
  public final List<OutPort> outPorts() {
    return Collections.emptyList();
  }
}
