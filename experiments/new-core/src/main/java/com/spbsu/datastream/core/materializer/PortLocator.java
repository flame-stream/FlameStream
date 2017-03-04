package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.graph.OutPort;

import java.util.Optional;

@FunctionalInterface
public interface PortLocator {
  Optional<DataSink> sinkForPort(OutPort port);

  default PortLocator compose(PortLocator locator) {
    return port -> sinkForPort(port)
            .map(Optional::of).orElseGet(() -> locator.sinkForPort(port));
  }
}
