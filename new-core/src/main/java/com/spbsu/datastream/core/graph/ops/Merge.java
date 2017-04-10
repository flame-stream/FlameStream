package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.FanIn;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.List;

public final class Merge<R> extends FanIn<R> {
  public Merge(final List<HashFunction<?>> hashes) {
    super(hashes);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    final DataItem<?> newItem = new PayloadDataItem<>(handler.copyAndAppendLocal(item.meta(), false), item.payload());

    prePush(newItem, handler);
    handler.push(outPort(), newItem);
    ack(item, handler);
  }
}
