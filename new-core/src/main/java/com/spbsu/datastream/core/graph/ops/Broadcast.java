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
import java.util.stream.Stream;

public final class Broadcast<T> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final List<OutPort> broadcastPorts;

  public Broadcast(final HashFunction<T> hash, final int shape) {
    super();
    this.inPort = new InPort(hash);
    this.broadcastPorts = Stream.generate(OutPort::new).limit((long) shape)
            .collect(Collectors.toList());
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    final List<OutPort> outPorts = this.outPorts();

    for (int i = 0; i < outPorts.size(); ++i) {
      final Meta newMeta = new Meta(item.meta(), this.incrementLocalTimeAndGet(), i);

      final DataItem<?> newItem = new PayloadDataItem<>(newMeta, item.payload());
      this.prePush(newItem, handler);
      handler.push(outPorts.get(i), item);
    }
    this.ack(item, handler);
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(this.inPort);
  }

  public InPort inPort() {
    return this.inPort;
  }

  public List<OutPort> broadcastPorts() {
    return Collections.unmodifiableList(this.broadcastPorts);
  }

  @Override
  public List<OutPort> outPorts() {
    final List<OutPort> outPorts = new ArrayList<>();
    outPorts.addAll(this.broadcastPorts);
    outPorts.add(this.ackPort());
    return Collections.unmodifiableList(outPorts);
  }

  @Override
  public String toString() {
    return "Broadcast{" + super.toString() + '}';
  }
}

