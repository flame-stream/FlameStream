package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.Set;

/**
 * Created by marnikitta on 2/6/17.
 */
public abstract class Sink<T> implements AtomicGraph {
  private final InPort inPort = new InPort();

  @Override
  public Set<InPort> inPorts() {
    return Collections.singleton(inPort);
  }

  @Override
  public Set<OutPort> outPorts() {
    return Collections.emptySet();
  }

  public InPort inPort() {
    return inPort;
  }
}
