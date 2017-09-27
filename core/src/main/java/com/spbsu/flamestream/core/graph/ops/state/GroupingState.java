package com.spbsu.flamestream.core.graph.ops.state;

import com.spbsu.flamestream.core.data.DataItem;

import java.util.List;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 23.02.2017
 * Time: 12:16
 */
public interface GroupingState<T> {
  List<DataItem<T>> getGroupFor(DataItem<T> item);

  void forEach(Consumer<List<DataItem<T>>> procedure);
}
