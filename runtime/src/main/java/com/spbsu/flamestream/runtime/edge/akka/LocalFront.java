package com.spbsu.flamestream.runtime.edge.akka;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.edge.EdgeContext;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class LocalFront<T> implements Front, Consumer<T> {
  private final NavigableMap<GlobalTime, DataItem> log = new TreeMap<>();
  private final BlockingQueue<DataItem> queue = new ArrayBlockingQueue<>(1);
  private final EdgeId frontId;
  private boolean eos = false;

  private Consumer<Object> hole;

  private long index = 0;

  public LocalFront(EdgeContext context) {
    this.frontId = context.edgeId();
  }

  @Override
  public synchronized void onStart(Consumer<Object> consumer, GlobalTime from) {
    if (eos) {
      consumer.accept(new Heartbeat(new GlobalTime(Long.MAX_VALUE, frontId)));
    }
    index = Math.max(index, from.time());
    this.hole = consumer;
    queue.clear();
    queue.addAll(log.tailMap(from).values());
  }

  @Override
  public synchronized void onRequestNext() {
    final DataItem item = queue.poll();
    if (item != null) {
      hole.accept(item);
      hole.accept(new Heartbeat(item.meta().globalTime()));
    }
  }

  @Override
  public synchronized void onCheckpoint(GlobalTime to) {
    log.headMap(to).clear();
  }

  @Override
  public synchronized void accept(T value) {
    try {
      final DataItem item = new PayloadDataItem(new Meta(new GlobalTime(++index, frontId)), value);
      log.put(item.meta().globalTime(), item);
      queue.put(item);
      if (queue.size() == 1) {
        onRequestNext();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public synchronized void eos() {
    if (hole == null) {
      eos = true;
    } else {
      hole.accept(new Heartbeat(new GlobalTime(Long.MAX_VALUE, frontId)));
    }
  }
}
