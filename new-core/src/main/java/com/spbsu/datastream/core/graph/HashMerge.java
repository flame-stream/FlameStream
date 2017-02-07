package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Created by marnikitta on 2/7/17.
 */
public class HashMerge extends AtomicGraph {
  private final InPort inPort = new InPort();
  private final OutPort outPort = new OutPort();

  @Override
  public Set<InPort> inPorts() {
    return Collections.singleton(inPort);
  }

  @Override
  public Set<OutPort> outPorts() {
    return Collections.singleton(outPort);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final HashMerge hashMerge = (HashMerge) o;
    return Objects.equals(inPort, hashMerge.inPort) &&
            Objects.equals(outPort, hashMerge.outPort);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inPort, outPort);
  }
}
