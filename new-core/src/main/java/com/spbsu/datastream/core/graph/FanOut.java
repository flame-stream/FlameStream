package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.HashFunction;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class FanOut<T> implements AtomicGraph {
  private final InPort inPort;
  private final List<OutPort> outPorts;

  protected FanOut(final HashFunction<? super T> hash, final int shape) {
    this.inPort = new InPort(hash);
    this.outPorts = Stream.generate(OutPort::new).limit(shape)
            .collect(Collectors.toList());
  }

  protected FanOut(final int shape) {
    this.inPort = new InPort(HashFunction.OBJECT_HASH);
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
}
