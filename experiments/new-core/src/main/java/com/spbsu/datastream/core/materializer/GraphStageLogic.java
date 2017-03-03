package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.ops.State;

/**
 * Created by marnikitta on 2/8/17.
 */
public abstract class GraphStageLogic {
  private ShardConcierge context;

  public <E> void onStart() {
  }

  public void onPush(InPort inPort, DataItem item) {
  }

  final protected void push(final OutPort out, final DataItem result) {
    context.portLocator().portSink(out).accept(result);
  }

  final protected <S extends State> void commitState(final S state) {
    // TODO: 3/3/17
  }

  final protected void panic(Exception e) {
    // TODO: 3/3/17
  }


  void injectConcierge(final ShardConcierge context) {
    this.context = context;
  }
}
