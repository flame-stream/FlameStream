package com.spbsu.flamestream.core.graph.barrier;

import com.spbsu.flamestream.core.graph.*;
import com.spbsu.flamestream.core.graph.atomic.AtomicGraph;
import com.spbsu.flamestream.core.graph.composed.ComposedGraph;

import java.util.List;
import java.util.Objects;
import java.util.function.ToIntFunction;

import static java.util.Collections.emptyList;

public final class BarrierSuite<T> implements Graph {
  private final PreBarrierMetaFilter<T> preBarrierMetaFilter;
  private final BarrierSink barrierSink;

  public BarrierSuite(AtomicGraph sink) {
    this.barrierSink = new BarrierSink(sink);
    //noinspection unchecked
    this.preBarrierMetaFilter = new PreBarrierMetaFilter<>(
            (ToIntFunction<? super T>) sink.inPorts().get(0).hashFunction()
    );
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
    return preBarrierMetaFilter.fuse(barrierSink, preBarrierMetaFilter.outPort(), barrierSink.inPort()).flattened();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final BarrierSuite<?> that = (BarrierSuite<?>) o;
    return Objects.equals(preBarrierMetaFilter, that.preBarrierMetaFilter) &&
            Objects.equals(barrierSink, that.barrierSink);
  }

  @Override
  public int hashCode() {
    return Objects.hash(preBarrierMetaFilter, barrierSink);
  }
}
