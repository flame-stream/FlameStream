package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.feedback.Ack;
import com.spbsu.datastream.core.feedback.NoAckDataItem;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.Collections;
import java.util.List;

public final class AckingGraph implements AtomicGraph {
  private final OutPort ackPort = new OutPort();

  @Override
  public List<InPort> inPorts() {
    return Collections.emptyList();
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(ackPort);
  }

  public OutPort ackPort() {
    return ackPort;
  }

  public void ack(final InPort port, final DataItem<?> dataItem, final AtomicHandle handle) {
    final Ack ack = new Ack(dataItem.meta().globalTime(), dataItem.meta().rootHash(), dataItem.ackHashCode());
    final DataItem<Ack> di = new NoAckDataItem<>(handle.copyAndAppendLocal(dataItem.meta(), false), ack);
    handle.push(ackPort, di);
  }
}
