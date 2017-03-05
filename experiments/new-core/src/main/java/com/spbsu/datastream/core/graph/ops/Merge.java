package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.FanIn;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.materializer.GraphStageLogic;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class Merge<T> extends FanIn {
  public Merge(final int n) {
    super(n);
  }

  @Override
  public String toString() {
    return "Merge{" + super.toString() + '}';
  }

  @Override
  public GraphStageLogic<T, T> logic() {
    return new GraphStageLogic<T, T>() {
      @Override
      public void onPush(final InPort inPort, final DataItem<T> item) {
        push(outPort(), item);
      }
    };
  }

  @Override
  public Graph deepCopy() {
    return new Merge(inPorts().size());
  }
}
