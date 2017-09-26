package com.spbsu.flamestream.core.graph.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.PayloadDataItem;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.meta.Meta;
import com.spbsu.flamestream.core.graph.AtomicHandle;

import java.util.Collections;
import java.util.List;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Broadcast<T> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final List<OutPort> broadcastPorts;

  public Broadcast(ToIntFunction<? super T> hash, int shape) {
    this.inPort = new InPort(hash);
    this.broadcastPorts = Stream.generate(OutPort::new).limit((long) shape)
            .collect(Collectors.toList());
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    final List<OutPort> outPorts = this.outPorts();
    final int newLocalTime = this.incrementLocalTimeAndGet();
    for (int i = 0; i < outPorts.size(); ++i) {
      final Meta newMeta = item.meta().advanced(newLocalTime, i);

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

