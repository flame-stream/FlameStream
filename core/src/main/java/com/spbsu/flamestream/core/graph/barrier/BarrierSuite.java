package com.spbsu.flamestream.core.graph.barrier;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.ComposedGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;

import java.util.List;
import java.util.function.ToIntFunction;

import static java.util.Collections.emptyList;

public final class BarrierSuite<T> implements Graph {
  private final PreBarrierMetaFilter<T> preBarrierMetaFilter;
  private final BarrierSink barrierSink;

  public BarrierSuite(AtomicGraph sink) {
    this.barrierSink = new BarrierSink(sink);
    this.preBarrierMetaFilter = new PreBarrierMetaFilter<>((ToIntFunction<? super T>)sink.inPorts().get(0).hashFunction());
  }

  public InPort inPort() {
    return preBarrierMetaFilter.inPort();
  }

  @Override
  public List<OutPort> outPorts() {
    return emptyList();
  }

  @Override
  public List<InPort> inPorts() {
    return preBarrierMetaFilter.inPorts();
  }

  @Override
  public ComposedGraph<AtomicGraph> flattened() {
    return preBarrierMetaFilter
            .fuse(barrierSink, preBarrierMetaFilter.outPort(), barrierSink.inPort())
            .flattened();
  }
}
