package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Created by marnikitta on 2/6/17.
 */
public abstract class Sink implements Graph {
  private final InPort inPort = new InPort();

  @Override
  public Set<InPort> inPorts() {
    return Collections.singleton(inPort);
  }

  @Override
  public Set<OutPort> outPorts() {
    return Collections.emptySet();
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
