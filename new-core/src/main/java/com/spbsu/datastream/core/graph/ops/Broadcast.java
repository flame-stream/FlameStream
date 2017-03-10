package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.FanOut;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class Broadcast<T> extends FanOut<T> {
  public Broadcast(final int shape) {
    super(shape);
  }

  @Override
  public String toString() {
    return "Broadcast{" + super.toString() + '}';
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    for (OutPort out : outPorts()) {
      handler.push(out, item);
    }
  }

  @Override
  public Graph deepCopy() {
    return new Broadcast(outPorts().size());
  }
}
