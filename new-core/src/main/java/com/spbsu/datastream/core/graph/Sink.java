package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.HashFunction;

import java.util.Collections;
import java.util.List;

public abstract class Sink<T> extends AckingGraph {
  private final InPort inPort;

  protected Sink(final HashFunction<? super T> hash) {
    this.inPort = new InPort(hash);
  }

  protected Sink() {
    this.inPort = new InPort(HashFunction.OBJECT_HASH);
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(inPort);
  }

  @Override
  public List<OutPort> outPorts() {
    return super.outPorts();
  }

  public InPort inPort() {
    return inPort;
  }
}
