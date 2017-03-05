package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;

/**
 * Created by marnikitta on 2/8/17.
 */
public abstract class GraphStageLogic<T, R> {
  private TickContext context;

  public void onStart() {
  }

  public void onPush(final InPort inPort, final DataItem<T> item) {
  }

  final protected void push(final OutPort out, final DataItem<R> result) {
    context.portLocator().sinkForPort(out).orElseThrow(RuntimeException::new).accept(result);
  }

  final protected void panic(final Exception e) {
    // TODO: 3/3/17
    throw new RuntimeException(e);
  }

  //package local, its important
  void injectConcierge(final TickContext context) {
    this.context = context;
  }

  @Override
  public String toString() {
    return getClass().getName();
  }
}
