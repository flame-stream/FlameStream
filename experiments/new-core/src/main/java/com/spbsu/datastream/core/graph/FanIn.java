package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by marnikitta on 2/7/17.
 */
public abstract class FanIn implements AtomicGraph {
  private final List<InPort> inPorts;
  private final OutPort outPort;

  public FanIn(final int shape) {
    this.inPorts = Stream.generate(InPort::new)
            .limit(shape).collect(Collectors.toList());
    this.outPort = new OutPort();
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.unmodifiableList(inPorts);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
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
