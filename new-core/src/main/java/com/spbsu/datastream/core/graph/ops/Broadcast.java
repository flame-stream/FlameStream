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
import java.util.stream.Stream;

public final class Broadcast<T> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final List<OutPort> broadcastPorts;

  public Broadcast(HashFunction<? super T> hash, int shape) {
    this.inPort = new InPort(hash);
    this.broadcastPorts = Stream.generate(OutPort::new).limit((long) shape)
            .collect(Collectors.toList());
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    final List<OutPort> outPorts = this.outPorts();
    final int newLocalTime = this.incrementLocalTimeAndGet();
    for (int i = 0; i < outPorts.size(); ++i) {
      final Meta newMeta = new Meta(item.meta(), newLocalTime, i);

      final DataItem<?> newItem = new PayloadDataItem<>(newMeta, item.payload());
      handle.push(outPorts.get(i), newItem);
    }
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
    return Collections.unmodifiableList(this.broadcastPorts);
  }

  @Override
  public String toString() {
    return "Broadcast{" + super.toString() + '}';
  }
}

