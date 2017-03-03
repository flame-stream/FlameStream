package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.FanOut;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.materializer.GraphStageLogic;

@SuppressWarnings("unchecked")
public class HashwiseBroadcast<T> extends FanOut {
  private final Hash<T> hash;

  public HashwiseBroadcast(final int partitions, final Hash<T> hash) {
    super(partitions);
    this.hash = hash;
  }

  public Hash<T> hash() {
    return hash;
  }

  @Override
  public GraphStageLogic logic() {
    return new GraphStageLogic<T, T>() {
      @Override
      public void onPush(final InPort inPort, final DataItem<T> item) {
        final OutPort out = outPorts().get(hash.hash(item.payload()) % outPorts().size());
        push(out, item);
      }
    };
  }

  @Override
  public Graph deepCopy() {
    return new HashwiseBroadcast(outPorts().size(), hash);
  }
}
