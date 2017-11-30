package com.spbsu.flamestream.runtime.graph.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class MapJoba<T, R> implements VertexJoba<T> {
  private final FlameMap<T, R> map;
  private final Consumer<DataItem<R>> sink;

  public MapJoba(FlameMap<T, R> map, Consumer<DataItem<R>> sink) {
    this.map = map;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem<T> dataItem) {
    map.operation().apply(dataItem).forEach(sink);
  }

  @Override
  public void onMinTime(GlobalTime globalTime) {

  }

  @Override
  public void onCommit() {

  }

}
