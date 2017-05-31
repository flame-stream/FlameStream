package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

public final class Filter<T> extends AbstractAtomicGraph {
  public static final String NULL_PAYLOAD = "empty";

  private final InPort inPort;
  private final OutPort outPort = new OutPort();
  private final OutPort nullPort = new OutPort();

  private final Predicate<T> predicate;

  public Filter(Predicate<T> predicate, HashFunction<? super T> hash) {
    this.inPort = new InPort(hash);
    this.predicate = predicate;
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handler) {
    @SuppressWarnings("unchecked") final boolean test = this.predicate.test((T) item.payload());

    if (test) {
      final DataItem<?> result = new PayloadDataItem<>(new Meta(item.meta(), this.incrementLocalTimeAndGet()), item.payload());
      handler.push(this.outPort(), result);
    } else {
      handler.push(this.nullPort(), new PayloadDataItem<>(new Meta(item.meta(), this.incrementLocalTimeAndGet()), Filter.NULL_PAYLOAD));
    }
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

  public OutPort nullPort() {
    return this.nullPort;
  }

  @Override
  public List<OutPort> outPorts() {
    final List<OutPort> result = new ArrayList<>();
    result.add(this.outPort);
    result.add(this.nullPort);
    return Collections.unmodifiableList(result);
  }
}

