package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class Merge<R> extends AbstractAtomicGraph {
  private final List<InPort> inPorts;
  private final OutPort outPort = new OutPort();

  @SuppressWarnings("TypeMayBeWeakened")
  public Merge(final List<HashFunction<?>> hashes) {
    this.inPorts = hashes.stream().map(InPort::new).collect(Collectors.toList());
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    final DataItem<?> newItem = new PayloadDataItem<>(new Meta(item.meta(), this.incrementLocalTimeAndGet()),
            item.payload());

    handler.push(this.outPort(), newItem);
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.unmodifiableList(this.inPorts);
  }

  public OutPort outPort() {
    return this.outPort;
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(this.outPort);
  }
}
