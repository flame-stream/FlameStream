package com.spbsu.flamestream.runtime.node.graph.materialization.vertices.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.runtime.node.graph.materialization.vertices.VertexMaterialization;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class MapMaterialization implements VertexMaterialization {
  private final FlameMap<?, ?> map;
  private final Consumer<DataItem<?>> sink;

  public MapMaterialization(FlameMap<?, ?> map, VertexMaterialization sink) {
    this.map = map;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem<?> dataItem) {
    map.operation().apply(dataItem).forEach(sink);
  }

  @Override
  public void onMinTime(GlobalTime globalTime) {

  }

  @Override
  public void onCommit() {

  }

}
