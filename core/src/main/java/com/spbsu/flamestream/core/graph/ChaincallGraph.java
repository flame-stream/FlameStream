package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.common.Statistics;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ChaincallGraph extends AbstractAtomicGraph {
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ChaincallGraph that = (ChaincallGraph) o;
    return Objects.equals(composedGraph, that.composedGraph) &&
            Objects.equals(upstreams, that.upstreams);
  }

  @Override
  public int hashCode() {
    return Objects.hash(composedGraph, upstreams);
  }

  private final class ChainHandle implements AtomicHandle {
    private final AtomicHandle superHandle;

    private ChainHandle(AtomicHandle superHandle) {
      this.superHandle = superHandle;
    }

    @Override
    public void push(OutPort out, DataItem<?> result) {
      final InPort destination = composedGraph.downstreams().get(out);
      if (destination != null) {
        final AtomicGraph atomic = upstreams.get(destination);
        atomic.onPush(destination, result, this);
      } else {
        superHandle.push(out, result);
      }
    }

    @Override
    public void ack(long xor, GlobalTime globalTime) {
      //there is no need to ack from chain
    }

    @Override
    public void submitStatistics(Statistics stat) {
      superHandle.submitStatistics(stat);
    }

    @Override
    public void error(String format, Object... args) {
      superHandle.error(format, args);
    }
  }
}
