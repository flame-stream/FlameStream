package com.spbsu.flamestream.runtime.graph.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.Grouping;

import java.util.List;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class GroupingJoba<T> implements VertexJoba<T> {
  private final Grouping<T> grouping;
  private final Consumer<DataItem<List<T>>> sink;

  public GroupingJoba(Grouping<T> grouping, Consumer<DataItem<List<T>>> sink) {
    this.grouping = grouping;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem<T> dataItem) {

  }

  @Override
  public void onMinTime(GlobalTime globalTime) {

  }

  @Override
  public void onCommit() {
    // TODO: 29.11.2017 save state
  }
}
