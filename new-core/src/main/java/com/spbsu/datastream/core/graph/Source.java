package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.List;

/**
 * Created by marnikitta on 2/6/17.
 */
public abstract class Source<R> implements AtomicGraph {
  private final OutPort outPort = new OutPort();

  @Override
  public List<InPort> inPorts() {
    return Collections.emptyList();
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(this.outPort);
  }

  public OutPort outPort() {
    return outPort;
  }
}
