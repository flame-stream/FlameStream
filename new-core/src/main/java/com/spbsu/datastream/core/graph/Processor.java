package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Created by marnikitta on 2/7/17.
 */
public abstract class Processor implements AtomicGraph {
  private final InPort inPort = new InPort();
  private final OutPort outPort = new OutPort();

  public InPort inPort() {
    return inPort;
  }

  public OutPort outPort() {
    return outPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(inPort);
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final Processor processor = (Processor) o;
    return Objects.equals(inPort, processor.inPort) &&
            Objects.equals(outPort, processor.outPort);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inPort, outPort);
  }

  @Override
  public String toString() {
    return "Processor{" + "inPort=" + inPort +
            ", outPort=" + outPort +
            '}';
  }
}
