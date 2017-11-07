package com.spbsu.flamestream.core.graph.source;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.front.FrontSubscription;
import com.spbsu.flamestream.core.graph.*;

import java.util.Collections;
import java.util.List;

public final class Source<T> implements AtomicGraph, SourceHandle<T> {
  private final OutPort outPort = new OutPort();

  private AtomicHandle handle;
  private FrontSubscription frontSubscription;
  private int capacity = 100;

  @Override
  public void onStart(AtomicHandle handle) {
    handle.registerSource(this);
    this.handle = handle;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.emptyList();
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
  }

  @Override
  public ComposedGraph<AtomicGraph> flattened() {
    return new ComposedGraphImpl<>(Collections.singleton(this));
  }

  @Override
  public void onSubscribe(FrontSubscription subscription) {
    this.frontSubscription = subscription;
  }

  @Override
  public void onInput(DataItem<T> dataItem) {
    handle.push(outPort, dataItem);
    capacity--;
    if (capacity < 0) {
      frontSubscription.switchToPull();
    }
  }

  @Override
  public void onWatermark(GlobalTime watermark) {
    capacity++;
    frontSubscription.request(1);
  }

  @Override
  public void onError(Throwable throwable) {
    handle.error("Smth went wrong {}", throwable);
  }

  @Override
  public void onComplete() {

  }
}
