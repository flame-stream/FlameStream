package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.graph.OutPort;

import java.util.function.Consumer;

public interface PortLocator {
  Consumer<Object> portSink(OutPort port);

  void registerPort(OutPort port, Consumer<Object> consumer);
}
