package com.spbsu.datastream.core;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;
import com.spbsu.datastream.core.stat.Statistics;
import com.spbsu.datastream.core.tick.TickInfo;

import java.util.function.BiConsumer;

public final class FakeAtomicHandle implements AtomicHandle {
  private final BiConsumer<OutPort, DataItem<?>> pushConsumer;

  public FakeAtomicHandle(BiConsumer<OutPort, DataItem<?>> pushConsumer) {
    this.pushConsumer = pushConsumer;
  }

  @Override
  public ActorSelection actorSelection(ActorPath path) {
    return null;
  }

  @Override
  public void push(OutPort out, DataItem<?> result) {
    pushConsumer.accept(out, result);
  }

  @Override
  public void ack(DataItem<?> item) {
  }

  @Override
  public void submitStatistics(Statistics stat) {
  }

  @Override
  public TickInfo tickInfo() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void error(String format, Object... args) {
  }
}
