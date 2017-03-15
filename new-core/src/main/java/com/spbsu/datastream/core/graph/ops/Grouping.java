package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.Hashable;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

public class Grouping<T extends Hashable<? super T>> extends Processor<T, GroupingResult<? super T>> {
  // TODO: 3/14/17 HASHABLE INTERFACE FIX TO MAKE GROUPING EASY
  private final int window;

  public Grouping(final int window) {
    this.window = window;
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    //FIXME PLEASE
  }

  @Override
  public Graph deepCopy() {
    return new Grouping(window);
  }
}
