package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.FanOut;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.materializer.GraphStageLogic;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class Broadcast<T> extends FanOut {
  public Broadcast(final int shape) {
    super(shape);
  }

  @Override
  public String toString() {
    return "Broadcast{" + super.toString() + '}';
  }

  @Override
  public GraphStageLogic logic() {
    return new GraphStageLogic<T, T>() {
      @Override
      public void onPush(final InPort inPort, final DataItem<T> item) {
        for (OutPort out : outPorts()) {
          push(out, item);
        }
      }
    };
  }

  @Override
  public Graph deepCopy() {
    return new Broadcast(outPorts().size());
  }
}
