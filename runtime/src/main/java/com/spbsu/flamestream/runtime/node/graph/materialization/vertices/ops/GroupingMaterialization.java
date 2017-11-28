package com.spbsu.flamestream.runtime.node.graph.materialization.vertices.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.runtime.node.graph.materialization.vertices.VertexMaterialization;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class GroupingMaterialization implements VertexMaterialization {
  private final Grouping<?> grouping;
  private final Consumer<DataItem<?>> sink;

  public GroupingMaterialization(Grouping<?> grouping, Consumer<DataItem<?>> sink) {
    this.grouping = grouping;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem<?> dataItem) {

  }

  @Override
  public void onMinTime(GlobalTime globalTime) {

  }

  @Override
  public void onCommit() {

  }
}
