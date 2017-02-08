package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by marnikitta on 2/7/17.
 */
public abstract class FanIn implements AtomicGraph {
  private final Set<InPort> inPorts;
  private final OutPort outPort;

  public FanIn(final int shape) {
    this.inPorts = Stream.generate(InPort::new)
            .limit(shape).collect(Collectors.toSet());
    this.outPort = new OutPort();
  }

  @Override
  public Set<InPort> inPorts() {
    return Collections.unmodifiableSet(inPorts);
  }

  @Override
  public Set<OutPort> outPorts() {
    return Collections.singleton(outPort);
  }

  public OutPort outPort() {
    return outPort;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final FanIn fanIn = (FanIn) o;
    return Objects.equals(inPorts, fanIn.inPorts) &&
            Objects.equals(outPort, fanIn.outPort);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inPorts, outPort);
  }

  @Override
  public String toString() {
    return "FanIn{" + "inPorts=" + inPorts +
            ", outPort=" + outPort +
            '}';
  }
}
