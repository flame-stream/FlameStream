package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public final class ConsumerBarrierSink<T> extends AbstractAtomicGraph {
  private final Consumer<T> consumer;
  private final InPort inPort;

  private final BarrierCollector collector = new LinearCollector();

  public ConsumerBarrierSink(final Consumer<T> consumer) {
    this.consumer = consumer;
    this.inPort = new InPort(PreSinkMetaElement.HASH_FUNCTION);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onMinGTimeUpdate(final GlobalTime globalTime, final AtomicHandle handle) {
    this.collector.update(globalTime);
    this.collector.released().stream().map(i -> (T) i).forEach(this.consumer);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    this.collector.enqueue(item);
  }

  public InPort inPort() {
    return this.inPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(this.inPort);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.emptyList();
  }
}
