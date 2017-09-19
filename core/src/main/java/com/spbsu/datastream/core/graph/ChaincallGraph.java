package com.spbsu.datastream.core.graph;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;
import com.spbsu.datastream.core.stat.Statistics;
import com.spbsu.datastream.core.tick.TickInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ChaincallGraph implements AtomicGraph {
  private final ComposedGraph<AtomicGraph> composedGraph;
  private final Map<InPort, AtomicGraph> upstreams = new HashMap<>();

  public ChaincallGraph(ComposedGraph<AtomicGraph> composedGraph) {
    this.composedGraph = composedGraph;

    for (AtomicGraph atomicGraph : composedGraph.subGraphs()) {
      for (InPort port : atomicGraph.inPorts()) {
        upstreams.put(port, atomicGraph);
      }
    }
  }

  @Override
  public void onStart(AtomicHandle handle) {
    composedGraph.subGraphs().forEach(atomic -> atomic.onStart(handle));
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    final AtomicGraph atomic = upstreams.get(inPort);
    atomic.onPush(inPort, item, new ChainHandle(handle));
  }

  @Override
  public void onCommit(AtomicHandle handle) {
    composedGraph.subGraphs().forEach(atomic -> atomic.onCommit(handle));
  }

  @Override
  public void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
    composedGraph.subGraphs().forEach(atomic -> atomic.onMinGTimeUpdate(globalTime, new ChainHandle(handle)));
  }

  @Override
  public List<InPort> inPorts() {
    return composedGraph.inPorts();
  }

  @Override
  public List<OutPort> outPorts() {
    return composedGraph.outPorts();
  }

  @Override
  public ComposedGraph<AtomicGraph> flattened() {
    return new ComposedGraphImpl<>(Collections.singleton(this));
  }

  private final class ChainHandle implements AtomicHandle {
    private final AtomicHandle superHandle;

    private ChainHandle(AtomicHandle superHandle) {
      this.superHandle = superHandle;
    }

    @Override
    public ActorSelection actorSelection(ActorPath path) {
      return superHandle.actorSelection(path);
    }

    //TODO: refactor atomic handle
    @Override
    public void push(OutPort out, DataItem<?> result) {
      final InPort destination = ChaincallGraph.this.composedGraph.downstreams().get(out);
      if (destination != null) {
        final AtomicGraph atomic = ChaincallGraph.this.upstreams.get(destination);
        atomic.onPush(destination, result, this);
      } else {
        superHandle.push(out, result);
      }
    }

    @Override
    public void ack(DataItem<?> item) {
      superHandle.ack(item);
    }

    @Override
    public void submitStatistics(Statistics stat) {
      superHandle.submitStatistics(stat);
    }

    @Override
    public TickInfo tickInfo() {
      return superHandle.tickInfo();
    }

    @Override
    public void error(String format, Object... args) {
      superHandle.error(format, args);
    }
  }
}
