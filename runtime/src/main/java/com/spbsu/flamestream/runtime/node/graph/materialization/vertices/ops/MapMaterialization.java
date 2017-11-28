package com.spbsu.flamestream.runtime.node.graph.materialization.vertices.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.runtime.node.graph.materialization.vertices.VertexMaterialization;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class MapMaterialization extends VertexMaterialization.Stub {
  public MapMaterialization(FlameMap<?, ?> map) {
    super(map);
  }

  @Override
  public Stream<DataItem<?>> apply(DataItem<?> dataItem) {
    return ((FlameMap<?, ?>) vertex).operation().apply(dataItem);
  }
}
