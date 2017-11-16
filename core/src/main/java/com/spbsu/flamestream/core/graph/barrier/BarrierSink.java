package com.spbsu.flamestream.core.graph.barrier;

import com.spbsu.flamestream.common.Statistics;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.ChaincallGraph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

class BarrierSink extends AbstractAtomicGraph {
  private final ChaincallGraph innerGraph;

  BarrierSink(AtomicGraph sink) {
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

  InPort inPort() {
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

  @Override
  public String toString() {
    return "BarrierSink{" + "innerGraph=" + innerGraph + '}';
  }

  private static class Barrier extends AbstractAtomicGraph {
    private final InPort inPort;
    private final OutPort outPort;

    // FIXME: 14.11.2017 
    //private final BarrierStatistics barrierStatistics = new BarrierStatistics();
    private final BarrierCollector collector = new BarrierCollector();

    Barrier() {
      this.inPort = new InPort(PreBarrierMetaElement.HASH_FUNCTION);
      this.outPort = new OutPort();
    }

    @Override
    public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
      collector.enqueue(item);
      //barrierStatistics.enqueue(item.meta().globalTime());
    }

    @Override
    public void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
      collector.releaseFrom(globalTime, di -> {
        final Object data = ((PreBarrierMetaElement<?>) di.payload()).payload();
        final DataItem<Object> dataItem = new PayloadDataItem<>(di.meta(), data);
        handle.push(outPort, dataItem);
        handle.ack(dataItem.xor(), dataItem.meta().globalTime());
        //barrierStatistics.release(di.meta().globalTime());
      });
    }

    @Override
    public void onCommit(AtomicHandle handle) {
      //handle.submitStatistics(barrierStatistics);
      if (!collector.isEmpty()) {
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

  private static class BarrierStatistics implements Statistics {
    private final TObjectLongMap<GlobalTime> timeMeasure = new TObjectLongHashMap<>();
    private final LongSummaryStatistics duration = new LongSummaryStatistics();

    void enqueue(GlobalTime globalTime) {
      timeMeasure.put(globalTime, System.nanoTime());
    }

    void release(GlobalTime globalTime) {
      final long start = timeMeasure.get(globalTime);
      if (start != Constants.DEFAULT_LONG_NO_ENTRY_VALUE) {
        duration.accept(System.nanoTime() - start);
        timeMeasure.remove(globalTime);
      }
    }

    @Override
    public Map<String, Double> metrics() {
      return Statistics.asMap("Barrier releasing duration", duration);
    }

    @Override
    public String toString() {
      return metrics().toString();
    }
  }
}
