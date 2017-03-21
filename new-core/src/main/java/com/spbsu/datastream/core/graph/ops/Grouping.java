package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

public class Grouping<T> extends Processor<T, GroupingResult<? super T>> {
  private final int window;

  private final HashFunction<T> hash;

  public Grouping(final HashFunction<T> hash, final int window) {
    this.window = window;
    this.hash = hash;
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    //COPYPASTE ME PLEASE
  }

  @Override
  public Graph deepCopy() {
    return new Grouping<>(hash, window);
  }
}
