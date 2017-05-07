package com.spbsu.datastream.core;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.Optional;
import java.util.function.BiConsumer;

public final class FakeAtomicHandle implements AtomicHandle {
  private final BiConsumer<OutPort, DataItem<?>> pushConsumer;

  public FakeAtomicHandle(final BiConsumer<OutPort, DataItem<?>> pushConsumer) {
    this.pushConsumer = pushConsumer;
  }

  @Override
  public ActorSelection actorSelection(final ActorPath path) {
    return null;
  }

  @Override
  public void push(final OutPort out, final DataItem<?> result) {
    this.pushConsumer.accept(out, result);
  }

  @Override
  public void ack(final DataItem<?> item) {

  }

  public Optional<Object> loadState(final InPort inPort) {
    return Optional.empty();
  }

  @Override
  public void saveState(final InPort inPort, final Object state) {
    throw new UnsupportedOperationException("Saving of the grouping state has not implemented yet");
  }

  @Override
  public void removeState(final InPort inPort) {
    throw new UnsupportedOperationException("Removing of the grouping state has not implemented yet");
  }

  @Override
  public HashRange localRange() {
    return new HashRange(Integer.MIN_VALUE, Integer.MAX_VALUE);
  }
}
