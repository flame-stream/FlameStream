package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.graph.FanIn;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.List;

public final class Merge<R> extends FanIn<R> {
  public Merge(final List<HashFunction<?>> hashes) {
    super(hashes);
  }

  public Merge(final int shape) {
    super(shape);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    handler.push(outPort(), item);
    handler.ack(inPort, item);
  }


}
