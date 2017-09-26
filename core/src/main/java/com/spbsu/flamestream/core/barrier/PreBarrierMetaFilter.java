package com.spbsu.flamestream.core.barrier;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.PayloadDataItem;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.graph.AtomicHandle;

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
            item.meta().advanced(incrementLocalTimeAndGet()),
            new PreBarrierMetaElement<>(item.payload(), HashFunction.UNIFORM_OBJECT_HASH.hash(item.meta().globalTime().front())));
    handle.push(outPort(), newItem);
  }

  public OutPort outPort() {
    return outPort;
  }

  public InPort inPort() {
    return inPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(inPort);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
  }
}
