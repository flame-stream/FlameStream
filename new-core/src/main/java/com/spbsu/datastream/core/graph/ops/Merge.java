package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.FanIn;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

public final class Merge<R> extends FanIn<R> {
  public Merge(final int n) {
    super(n);
  }

  @Override
  public String toString() {
    return "Merge{" + super.toString() + '}';
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    handler.push(outPort(), item);
    handler.ack(item);
  }

  @Override
  public Graph deepCopy() {
    return new Merge(inPorts().size());
  }
}
