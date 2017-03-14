package com.spbsu.datastream.core.materializer.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.TheGraph;

public interface AtomicHandle {
  void deploy(final TheGraph graph);

  void push(final OutPort out, final DataItem<?> result);

  void panic(final Exception e);
}

