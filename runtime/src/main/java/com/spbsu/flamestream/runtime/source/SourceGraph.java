package com.spbsu.flamestream.runtime.source;

import akka.actor.AbstractActor;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.ComposedGraph;
import com.spbsu.flamestream.core.graph.ComposedGraphImpl;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.runtime.actor.LoggingActor;

import java.util.Collections;
import java.util.List;

public final class SourceGraph<T> implements AtomicGraph {
  private final OutPort outPort = new OutPort();
  private volatile long systemLimit = 100;

  @Override
  public void onStart(AtomicHandle handle) {
  }

  @Override
  public void onCommit(AtomicHandle handle) {
  }

  @Override
  public void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.emptyList();
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
  }

  @Override
  public ComposedGraph<AtomicGraph> flattened() {
    return new ComposedGraphImpl<>(Collections.singleton(this));
  }

}
