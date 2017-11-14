package com.spbsu.flamestream.core.graph.ops;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.atomic.impl.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.atomic.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

public final class FlatMap<T, R> extends AbstractAtomicGraph {
  private final Function<T, Stream<R>> function;

  private final OutPort outPort = new OutPort();

  private final InPort inPort;

  public FlatMap(Function<T, Stream<R>> function, ToIntFunction<? super T> hash) {
    this.function = function;
    this.inPort = new InPort(hash);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handler) {
    final Stream<R> res = function.apply((T) item.payload());
    final int newLocalTime = incrementLocalTimeAndGet();

    final int[] childId = {0};
    final long[] xor = {0};
    res.forEach(t -> {
      final Meta newMeta = item.meta().advanced(newLocalTime, childId[0]);
      final DataItem<R> newDataItem = new PayloadDataItem<>(newMeta, t);

      handler.push(outPort(), newDataItem);
      xor[0] ^= newDataItem.ack();

      childId[0]++;
    });
    handler.ack(xor[0], item.meta().globalTime());
  }

  public InPort inPort() {
    return inPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(inPort);
  }

  public OutPort outPort() {
    return outPort;
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
  }
}
