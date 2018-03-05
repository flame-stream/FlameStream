package com.spbsu.flamestream.runtime.edge.akka;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.edge.EdgeContext;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class LocalFront<T> implements Front, Consumer<T> {
  private final ConcurrentNavigableMap<GlobalTime, DataItem> log = new ConcurrentSkipListMap<>();
  private final EdgeId frontId;

  private final AtomicInteger requestDebt = new AtomicInteger(0);
  private volatile boolean eos = false;
  private volatile Consumer<Object> hole;

  private GlobalTime lastEmitted = GlobalTime.MIN;

  private long index = 0;

  public LocalFront(EdgeContext context) {
    this.frontId = context.edgeId();
  }

  @Override
  public void onStart(Consumer<Object> consumer, GlobalTime from) {
    if (eos) {
      consumer.accept(new Heartbeat(new GlobalTime(Long.MAX_VALUE, frontId)));
    }
    index = Math.max(index, from.time());
    this.hole = consumer;
  }

  @Override
  public void onRequestNext() {
    requestDebt.incrementAndGet();
    tryPoll();
  }

  private void tryPoll() {
    final Map.Entry<GlobalTime, DataItem> entry = log.higherEntry(lastEmitted);
    if (entry != null) {
      requestDebt.decrementAndGet();
      hole.accept(entry.getValue());
      hole.accept(new Heartbeat(entry.getKey()));
      lastEmitted = entry.getKey();
    }
  }

  @Override
  public void onCheckpoint(GlobalTime to) {
    log.headMap(to).clear();
  }

  @Override
  public void accept(T value) {
    final DataItem item = new PayloadDataItem(new Meta(new GlobalTime(++index, frontId)), value);
    log.put(item.meta().globalTime(), item);
    if (requestDebt.get() > 0) {
      tryPoll();
    }
  }

  public void eos() {
    if (hole == null) {
      eos = true;
    } else {
      hole.accept(new Heartbeat(new GlobalTime(Long.MAX_VALUE, frontId)));
    }
  }
}
