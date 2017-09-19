package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.Collections;
import java.util.List;
import java.util.function.ToIntFunction;

public final class PreBarrierMetaFilter<T> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final OutPort outPort = new OutPort();

  public PreBarrierMetaFilter(ToIntFunction<? super T> hashFunction) {
    this.inPort = new InPort(hashFunction);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    final DataItem<?> newItem = new PayloadDataItem<>(
            item.meta().advanced(this.incrementLocalTimeAndGet()),
            new PreBarrierMetaElement<>(item.payload(), HashFunction.UNIFORM_OBJECT_HASH.hash(item.meta().globalTime().front())));
    handle.push(this.outPort(), newItem);
  }

  public OutPort outPort() {
    return this.outPort;
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
    return Collections.singletonList(this.outPort);
  }
}
