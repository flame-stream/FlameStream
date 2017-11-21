package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.utils.Statistics;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.OutPort;

import java.util.function.BiConsumer;

public final class FakeAtomicHandle implements AtomicHandle {
  private final BiConsumer<OutPort, DataItem<?>> pushConsumer;

  public FakeAtomicHandle(BiConsumer<OutPort, DataItem<?>> pushConsumer) {
    this.pushConsumer = pushConsumer;
  }

  @Override
  public void push(OutPort out, DataItem<?> result) {
    pushConsumer.accept(out, result);
  }

  @Override
  public void ack(long xor, GlobalTime globalTime) {
  }

  @Override
  public void submitStatistics(Statistics stat) {
  }

  @Override
  public void error(String format, Object... args) {
  }
}
