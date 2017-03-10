package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.FanOut;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

public class HashwiseBroadcast<T> extends FanOut<T> {
  private final Hash<T> hash;

  public HashwiseBroadcast(final int partitions, final Hash<T> hash) {
    super(partitions);
    this.hash = hash;
  }

  public Hash<T> hash() {
    return hash;
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    @SuppressWarnings("unchecked")
    final OutPort out = outPorts().get(hash.hash((T) item.payload()) % outPorts().size());

    handler.push(out, item);
  }

  @Override
  public Graph deepCopy() {
    return new HashwiseBroadcast<>(outPorts().size(), hash);
  }
}
