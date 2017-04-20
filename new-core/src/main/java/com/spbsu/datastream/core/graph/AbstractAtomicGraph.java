package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.feedback.Ack;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.Collections;

public abstract class AbstractAtomicGraph implements AtomicGraph {
  private final OutPort ackPort = new OutPort();

  private int localTime = 0;

  protected final int incrementLocalTimeAndGet() {
    ++this.localTime;
    return this.localTime;
  }

  public final OutPort ackPort() {
    return this.ackPort;
  }

  protected final void ack(final DataItem<?> dataItem, final AtomicHandle handle) {
    final Ack ack = new Ack(dataItem.meta().globalTime().time(), dataItem.meta().globalTime().initHash(), dataItem.ack());
    final DataItem<Ack> di = new PayloadDataItem<>(new Meta(dataItem.meta(), this.incrementLocalTimeAndGet()), ack);
    handle.push(this.ackPort, di);
  }

  protected final void prePush(final DataItem<?> dataItem, final AtomicHandle handle) {
    this.ack(dataItem, handle);
  }

  @Override
  public final ComposedGraph<AtomicGraph> flattened() {
    return new ComposedGraphImpl<>(Collections.singleton(this));
  }
}
