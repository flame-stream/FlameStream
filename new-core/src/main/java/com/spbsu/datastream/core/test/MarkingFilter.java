package com.spbsu.datastream.core.test;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.concurrent.ThreadLocalRandom;

public class MarkingFilter extends Processor<Integer, Integer> {
  private final ThreadLocalRandom rd = ThreadLocalRandom.current();

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    final Integer value = ((Integer) item.payload());
    int result = rd.nextBoolean() ? 1 : -1;
    if (value >= 0) {
      result *= 2000;
    } else {
      result *= 1000;
    }

    final Integer marked = result;
    final DataItem<Integer> out = new PayloadDataItem<>(handle.copyAndAppendLocal(item.meta()), marked);
    handle.push(outPort(), out);
  }
}
