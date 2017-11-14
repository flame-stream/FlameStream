package com.spbsu.flamestream.core.graph.barrier;

import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.graph.atomic.impl.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.atomic.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;

import java.util.Collections;
import java.util.List;
import java.util.function.ToIntFunction;

final class PreBarrierMetaFilter<T> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final OutPort outPort = new OutPort();

  PreBarrierMetaFilter(ToIntFunction<? super T> hashFunction) {
    this.inPort = new InPort(hashFunction);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    final DataItem<?> newItem = new PayloadDataItem<>(
            item.meta().advanced(incrementLocalTimeAndGet()),
            new PreBarrierMetaElement<>(
                    item.payload(),
                    HashFunction.UNIFORM_OBJECT_HASH.hash(item.meta().globalTime().front())
            )
    );
    handle.push(outPort(), newItem);
    handle.ack(newItem.ack(), newItem.meta().globalTime());
  }

  OutPort outPort() {
    return outPort;
  }

  InPort inPort() {
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
