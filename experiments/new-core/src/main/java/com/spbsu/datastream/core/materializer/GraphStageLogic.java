package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;

/**
 * Created by marnikitta on 2/8/17.
 */
public abstract class GraphStageLogic<T, R> {
  private ShardConcierge context;

  public void onStart() {
  }

  public void onPush(final InPort inPort, final DataItem<T> item) {
  }

  final protected void push(final OutPort out, final DataItem<R> result) {
    context.portLocator().portSink(out).accept(result);
  }

  final protected void panic(final Exception e) {
    // TODO: 3/3/17
  }


  //package local, its important
  void injectConcierge(final ShardConcierge context) {
    this.context = context;
  }
}
