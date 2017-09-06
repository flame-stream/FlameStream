package com.spbsu.datastream.core;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;
import com.spbsu.datastream.core.stat.Statistics;
import com.spbsu.datastream.core.tick.TickInfo;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Optional;
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
    this.pushConsumer.accept(out, result);
  }

  @Override
  public void ack(DataItem<?> item) {

  }

  @Override
  public Optional<Object> loadState(InPort inPort) {
    return Optional.empty();
  }

  @Override
  public void saveState(InPort inPort, Object state) {
    throw new UnsupportedOperationException("Saving of the grouping state has not implemented yet");
  }

  @Override
  public void removeState(InPort inPort) {
    throw new UnsupportedOperationException("Removing of the grouping state has not implemented yet");
  }

  @Override
  public void submitStatistics(Statistics stat) {
  }

  @Override
  public TickInfo tickInfo() {
    throw new NotImplementedException();
  }
}
