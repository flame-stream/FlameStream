package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class MapJoba implements Joba {
  private final FlameMap<?, ?>.FlameMapOperation operation;

  public MapJoba(FlameMap<?, ?> flameMap) {
    this.operation = flameMap.operation(ThreadLocalRandom.current().nextLong());
  }

  @Override
  public void accept(DataItem item, Consumer<DataItem> sink) {
    operation.apply(item).forEach(sink);
  }
}
