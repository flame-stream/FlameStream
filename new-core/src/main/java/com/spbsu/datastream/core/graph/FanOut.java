package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by marnikitta on 2/7/17.
 */
public abstract class FanOut implements AtomicGraph {
  private final InPort inPort;
  private final List<OutPort> outPorts;

  public FanOut(final int shape) {
    this.inPort = new InPort();

    this.outPorts = Stream.generate(OutPort::new).limit(shape)
            .collect(Collectors.toList());
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(inPort);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.unmodifiableList(outPorts);
  }

  public InPort inPort() {
    return inPort;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final FanOut fanOut = (FanOut) o;
    return Objects.equals(inPort, fanOut.inPort) &&
            Objects.equals(outPorts, fanOut.outPorts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inPort, outPorts);
  }

  @Override
  public String toString() {
    return "FanOut{" + "inPort=" + inPort +
            ", outPorts=" + outPorts +
            '}';
  }
}
