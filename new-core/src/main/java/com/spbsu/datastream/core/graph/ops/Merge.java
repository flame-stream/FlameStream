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
import java.util.stream.Collectors;

public final class Merge<R> extends AbstractAtomicGraph {
  private final List<InPort> inPorts;
  private final OutPort outPort = new OutPort();

  public Merge(final List<HashFunction<?>> hashes) {
    super();
    this.inPorts = hashes.stream().map(InPort::new).collect(Collectors.toList());
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    final DataItem<?> newItem = new PayloadDataItem<>(new Meta(item.meta(), this.incrementLocalTimeAndGet()),
            item.payload());

    this.prePush(newItem, handler);
    handler.push(this.outPort(), newItem);
    this.ack(item, handler);
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
    final List<OutPort> result = new ArrayList<>();
    result.add(outPort);
    result.add(this.ackPort());

    return Collections.unmodifiableList(result);
  }
}
