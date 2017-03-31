package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

public class Grouping<T> extends Processor<T, GroupingResult<? super T>> {
  private final int window;

  public Grouping(final HashFunction<T> hash, final int window) {
    super(hash);
    this.window = window;
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    //COPYPASTE ME PLEASE
  }
}
