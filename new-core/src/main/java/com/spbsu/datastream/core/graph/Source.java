package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Created by marnikitta on 2/6/17.
 */
public abstract class Source implements Graph {
  private final OutPort outPort = new OutPort();

  @Override
  public Set<InPort> inPorts() {
    return Collections.emptySet();
  }

  @Override
  public Set<OutPort> outPorts() {
    return Collections.singleton(this.outPort);
  }

  @Override
  public Map<OutPort, InPort> downstreams() {
    return Collections.emptyMap();
  }

  @Override
  public Map<InPort, OutPort> upstreams() {
    return Collections.emptyMap();
  }
}
