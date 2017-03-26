package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.HashFunction;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class FanIn<R> implements AtomicGraph {
  private final List<InPort> inPorts;
  private final OutPort outPort;

  protected FanIn(final List<HashFunction<?>> hashes) {
    this.inPorts = hashes.stream().map(InPort::new).collect(Collectors.toList());
    this.outPort = new OutPort();
  }

  protected FanIn(final int shape) {
    this.inPorts = Stream.generate(() -> HashFunction.OBJECT_HASH).limit(shape)
            .map(InPort::new).collect(Collectors.toList());
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
}
