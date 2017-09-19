package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.*;
import com.spbsu.datastream.core.meta.GlobalTime;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public final class BarrierSink implements AtomicGraph {
  private final ChaincallGraph innerGraph;

  public BarrierSink(AtomicGraph sink) {
    if (!(sink.inPorts().size() == 1 && sink.outPorts().isEmpty())) {
      throw new IllegalArgumentException(format(
              "sink should have one input and no outputs, found %d inputs, %d outputs",
              sink.inPorts().size(),
              sink.outPorts().size()
      ));
    }

    final Barrier barrier = new Barrier();
    this.innerGraph = new ChaincallGraph(barrier.fuse(sink, barrier.outPort(), sink.inPorts().get(0)).flattened());
  }

  public InPort inPort() {
    return inPorts().get(0);
  }

  @Override
  public List<InPort> inPorts() {
    return innerGraph.inPorts();
  }

  @Override
  public List<OutPort> outPorts() {
    return innerGraph.outPorts();
  }

  @Override
  public ComposedGraph<AtomicGraph> flattened() {
    return innerGraph.flattened();
  }

  @Override
  public void onStart(AtomicHandle handle) {
    innerGraph.onStart(handle);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    innerGraph.onPush(inPort, item, handle);
  }

  @Override
  public void onCommit(AtomicHandle handle) {
    innerGraph.onCommit(handle);
  }

  @Override
  public void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
    innerGraph.onMinGTimeUpdate(globalTime, handle);
  }

  private static final class Barrier extends AbstractAtomicGraph {
    private final InPort inPort;
    private final OutPort outPort;

    private final BarrierCollector collector = new LinearCollector();

    Barrier() {
      this.inPort = new InPort(PreBarrierMetaElement.HASH_FUNCTION);
      this.outPort = new OutPort();
    }

    @Override
    public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
      this.collector.enqueue(item);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
      this.collector.releaseFrom(globalTime, di -> handle.push(outPort, di));
    }

    @Override
    public void onCommit(AtomicHandle handle) {
      if (!this.collector.isEmpty()) {
        throw new IllegalStateException("Barrier should be empty");
      }
    }

    OutPort outPort() {
      return outPort;
    }

    @Override
    public List<InPort> inPorts() {
      return singletonList(inPort);
    }

    @Override
    public List<OutPort> outPorts() {
      return singletonList(outPort);
    }
  }
}
