package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.Collections;
import java.util.List;

public final class PreSinkMetaFilter<T> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final OutPort outPort = new OutPort();

  public PreSinkMetaFilter(final HashFunction<? super T> hashFunction) {
    this.inPort = new InPort(hashFunction);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    final DataItem<?> newItem = new PayloadDataItem<>(
            new Meta(item.meta(), this.incrementLocalTimeAndGet()),
            new PreSinkMetaElement<>(item.payload(), PreSinkMetaFilter.jenkinsHash(item.meta().globalTime().front())));
    handle.push(this.outPort(), newItem);
  }

  @SuppressWarnings({"UnnecessaryParentheses", "MagicNumber"})
  private static int jenkinsHash(final int value) {
    int a = value;
    a = (a + 0x7ed55d16) + (a << 12);
    a = (a ^ 0xc761c23c) ^ (a >> 19);
    a = (a + 0x165667b1) + (a << 5);
    a = (a + 0xd3a2646c) ^ (a << 9);
    a = (a + 0xfd7046c5) + (a << 3);
    a = (a ^ 0xb55a4f09) ^ (a >> 16);
    return a;
  }

  public OutPort outPort() {
    return this.outPort;
  }

  public InPort inPort() {
    return this.inPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(this.inPort);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(this.outPort);
  }
}
