package com.spbsu.datastream.core.materializer.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.OutPort;

public interface AtomicHandle {
  void push(final OutPort out, final DataItem<?> result);

  void panic(final Exception e);
}

