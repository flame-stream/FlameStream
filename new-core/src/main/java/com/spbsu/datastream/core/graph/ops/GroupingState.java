package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 23.02.2017
 * Time: 12:16
 */
public interface GroupingState<T> {
  Optional<List<DataItem<T>>> get(DataItem<T> item);

  void put(List<DataItem<T>> dataItems);

  void forEach(Consumer<List<DataItem<T>>> procedure);
}
