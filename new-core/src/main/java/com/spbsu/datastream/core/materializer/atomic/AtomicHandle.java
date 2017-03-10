package com.spbsu.datastream.core.materializer.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.TheGraph;

import java.net.InetSocketAddress;
import java.util.List;

public interface AtomicHandle {
  List<InetSocketAddress> workers();

  void deploy(final TheGraph graph);

  void push(final OutPort out, final DataItem<?> result);

  void panic(final Exception e);
}

