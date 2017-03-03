package com.spbsu.datastream.core.graph.ops.grouping_storage;

import com.spbsu.datastream.core.DataItem;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 23.02.2017
 * Time: 12:16
 */
public interface GroupingStorage {
  Optional<List<DataItem>> get(DataItem item);

  void put(List<DataItem> dataItems);

  void forEach(Consumer<List<DataItem>> procedure);
}
