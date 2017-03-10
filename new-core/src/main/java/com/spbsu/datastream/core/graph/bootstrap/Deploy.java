package com.spbsu.datastream.core.graph.bootstrap;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Sink;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

public class Deploy extends Sink<TheGraph> {
  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    handle.deploy((TheGraph) item.payload());
  }

  @Override
  public Graph deepCopy() {
    return new Deploy();
  }
}
