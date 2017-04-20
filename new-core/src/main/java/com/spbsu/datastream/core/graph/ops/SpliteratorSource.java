package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class SpliteratorSource<T> extends AbstractAtomicGraph {
  private final OutPort outPort = new OutPort();

  private final Spliterator<T> spliterator;

  public SpliteratorSource(final Spliterator<T> spliterator) {
    super();
    this.spliterator = spliterator;
  }

  @Override
  public void onStart(final AtomicHandle handler) {
    final AtomicInteger currentId = new AtomicInteger();

    this.spliterator.forEachRemaining(item -> {
      final GlobalTime globalTime = new GlobalTime(System.currentTimeMillis(),
              handler.localRange().from());
      final Meta now = new Meta(globalTime, this.incrementLocalTimeAndGet());
      final DataItem<T> dataItem = new PayloadDataItem<>(now, item);

      this.prePush(dataItem, handler);
      handler.push(this.outPort(), dataItem);
      try {
        TimeUnit.MILLISECONDS.sleep(100L);
      } catch (final InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public OutPort outPort() {
    return this.outPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.emptyList();
  }

  @Override
  public List<OutPort> outPorts() {
    final List<OutPort> result = new ArrayList<>();
    result.add(this.outPort);
    result.add(this.ackPort());

    return Collections.unmodifiableList(result);
  }
}
