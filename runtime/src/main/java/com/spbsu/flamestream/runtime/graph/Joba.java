package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

public interface Joba {
  void accept(DataItem item, Consumer<DataItem> sink);

  default void onMinTime(GlobalTime time) {
  }
}
