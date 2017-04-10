package com.spbsu.datastream.core.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class Source<R> extends AckingGraph {
  private final OutPort outPort = new OutPort();

  @Override
  public List<InPort> inPorts() {
    return Collections.emptyList();
  }

  @Override
  public List<OutPort> outPorts() {
    final List<OutPort> outPorts = new ArrayList<>();
    outPorts.add(outPort);
    outPorts.addAll(super.outPorts());
    return Collections.unmodifiableList(outPorts);
  }

  public OutPort outPort() {
    return outPort;
  }
}
