package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class PreSinkMetaFilter<T> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final OutPort outPort = new OutPort();

  public PreSinkMetaFilter(final HashFunction<T> hashFunction) {
    super();
    this.inPort = new InPort(hashFunction);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    final DataItem<?> newItem = new PayloadDataItem<>(
            new Meta(item.meta(), this.incrementLocalTimeAndGet()),
            new PreSinkMetaElement<>(item.payload(), item.meta().globalTime().initHash()));
    this.prePush(newItem, handle);
    handle.push(this.outPort(), newItem);
    this.ack(item, handle);
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
    final List<OutPort> outPorts = new ArrayList<>();

    outPorts.add(outPort);
    outPorts.add(this.ackPort());

    return Collections.unmodifiableList(outPorts);
  }
}
