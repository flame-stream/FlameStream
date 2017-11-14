package com.spbsu.flamestream.core.graph.ops;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.graph.atomic.impl.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.atomic.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;

import java.util.Collections;
import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

public final class Merge extends AbstractAtomicGraph {
  private final List<InPort> inPorts;
  private final OutPort outPort = new OutPort();

  @SuppressWarnings("TypeMayBeWeakened")
  public Merge(List<? extends ToIntFunction<?>> hashes) {
    this.inPorts = hashes.stream().map(InPort::new).collect(Collectors.toList());
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handler) {
    final DataItem<?> newItem = new PayloadDataItem<>(item.meta().advanced(incrementLocalTimeAndGet()), item.payload());

    handler.push(outPort(), newItem);
    handler.ack(newItem.ack(), newItem.meta().globalTime());
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.unmodifiableList(inPorts);
  }

  public OutPort outPort() {
    return outPort;
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
  }
}
