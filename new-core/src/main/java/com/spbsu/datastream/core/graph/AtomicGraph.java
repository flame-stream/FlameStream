package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.Map;

/**
 * Created by marnikitta on 2/7/17.
 */
public abstract class AtomicGraph implements Graph {
  @Override
  public final Map<OutPort, InPort> downstreams() {
    return Collections.emptyMap();
  }

  @Override
  public final Map<InPort, OutPort> upstreams() {
    return Collections.emptyMap();
  }
}
