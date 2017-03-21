package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.List;

public abstract class Sink<T> implements AtomicGraph {
  private final InPort inPort = new InPort();

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(inPort);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.emptyList();
  }

  public InPort inPort() {
    return inPort;
  }
}
