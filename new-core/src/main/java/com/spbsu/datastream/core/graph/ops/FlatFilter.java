package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;
import org.jooq.lambda.Seq;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public final class FlatFilter<T, R> extends AbstractAtomicGraph {
  private final Function<T, Stream<R>> function;

  private final OutPort outPort = new OutPort();

  private final InPort inPort;

  public FlatFilter(final Function<T, Stream<R>> function, final HashFunction<T> hash) {
    this.function = function;
    this.inPort = new InPort(hash);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    final Stream<R> res = this.function.apply((T) item.payload());
    Seq.zipWithIndex(res).forEach(t -> {
      final Meta newMeta = new Meta(item.meta(), this.incrementLocalTimeAndGet(), Math.toIntExact(t.v2()));
      final DataItem<R> newDataItem = new PayloadDataItem<>(newMeta, t.v1());

      handler.push(this.outPort(), newDataItem);
    });
  }

  public InPort inPort() {
    return this.inPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(this.inPort);
  }

  public OutPort outPort() {
    return this.outPort;
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(this.outPort);
  }
}
