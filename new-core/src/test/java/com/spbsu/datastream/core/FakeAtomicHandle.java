package com.spbsu.datastream.core;

import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.ops.GroupingState;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.function.BiConsumer;

public final class FakeAtomicHandle implements AtomicHandle {
  private final BiConsumer<OutPort, DataItem<?>> pushConsumer;

  public FakeAtomicHandle(final BiConsumer<OutPort, DataItem<?>> pushConsumer) {
    this.pushConsumer = pushConsumer;
  }

  @Override
  public void push(final OutPort out, final DataItem<?> result) {
    this.pushConsumer.accept(out, result);
  }

  @Override
  public void ack(final DataItem<?> item) {

  }

  @Override
  public GroupingState<?> loadGroupingState() {
    // TODO: 4/21/17 return empty state
    throw new UnsupportedOperationException("Loading of the grouping state has not implemented yet");
  }

  @Override
  public void saveGroupingState(final GroupingState<?> storage) {
    throw new UnsupportedOperationException("Saving of the grouping state has not implemented yet");
  }

  @Override
  public HashRange localRange() {
    return new HashRange(Integer.MIN_VALUE, Integer.MAX_VALUE);
  }
}
