package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class BarrierSink<T> extends AbstractAtomicGraph {
  private final InPort inPort;

  private final InPort nullPort;

  private final BarrierCollector collector = new LinearCollector();

  public BarrierSink() {
    this.inPort = new InPort(PreSinkMetaElement.HASH_FUNCTION);
    this.nullPort = new InPort(PreSinkMetaElement.HASH_FUNCTION);
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

  protected abstract void consume(T payload);

  public final InPort inPort() {
    return this.inPort;
  }

  public final InPort nullPort() {
    return this.nullPort;
  }

  @Override
  public final List<InPort> inPorts() {
    final List<InPort> result = new ArrayList<>();
    result.add(this.inPort);
    result.add(this.nullPort);
    return Collections.unmodifiableList(result);
  }

  @Override
  public final List<OutPort> outPorts() {
    return Collections.emptyList();
  }
}
