package com.spbsu.flamestream.runtime.node.graph.materialization.vertices.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.runtime.node.graph.materialization.vertices.VertexMaterialization;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class GroupingMaterialization extends VertexMaterialization.Stub {
  private final Grouping grouping;

  public GroupingMaterialization(Grouping<?> grouping) {
    super(grouping.id());
    this.grouping = grouping;
  }

  @Override
  public Stream<DataItem<?>> apply(DataItem<?> dataItem) {
    return null;
  }
}
