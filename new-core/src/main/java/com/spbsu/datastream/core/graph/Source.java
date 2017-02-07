package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.Set;

/**
 * Created by marnikitta on 2/6/17.
 */
public abstract class Source<T> extends AtomicGraph {
  private final OutPort outPort = new OutPort();

  @Override
  public Set<InPort> inPorts() {
    return Collections.emptySet();
  }

  @Override
  public Set<OutPort> outPorts() {
    return Collections.singleton(this.outPort);
  }

  public OutPort outPort() {
    return outPort;
  }
}
