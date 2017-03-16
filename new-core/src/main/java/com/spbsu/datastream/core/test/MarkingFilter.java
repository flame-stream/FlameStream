package com.spbsu.datastream.core.test;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadHashDataItem;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.hashable.HashableInteger;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

import java.util.concurrent.ThreadLocalRandom;

public class MarkingFilter extends Processor<HashableInteger, HashableInteger> {
  private final ThreadLocalRandom rd = ThreadLocalRandom.current();

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handle) {
    final HashableInteger value = ((HashableInteger) item.payload());
    int result = rd.nextBoolean() ? 1 : -1;
    if (value.intValue() >= 0) {
      result *= 2000;
    } else {
      result *= 1000;
    }

    final HashableInteger marked = new HashableInteger(result);
    final DataItem<HashableInteger> out = new PayloadHashDataItem<>(Meta.now(), marked);
    handle.push(outPort(), out);
  }

  @Override
  public Graph deepCopy() {
    return new MarkingFilter();
  }
}
