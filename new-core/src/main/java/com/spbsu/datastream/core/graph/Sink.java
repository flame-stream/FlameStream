package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.HashFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class Sink<T> extends AckingGraph {
  private final InPort inPort;

  private final InPort feedbackPort;

  protected Sink(final HashFunction<? super T> hash, final HashFunction<?> initHash) {
    this.inPort = new InPort(hash);
    this.feedbackPort = new InPort(initHash);
  }

  @Override
  public List<InPort> inPorts() {
    final List<InPort> inPorts = new ArrayList<>();
    inPorts.add(inPort);
    inPorts.add(feedbackPort);
    return Collections.unmodifiableList(inPorts);
  }

  @Override
  public List<OutPort> outPorts() {
    return super.outPorts();
  }

  public InPort inPort() {
    return inPort;
  }
}
