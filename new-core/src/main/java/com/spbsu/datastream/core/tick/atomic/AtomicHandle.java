package com.spbsu.datastream.core.tick.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.ops.GroupingState;

public interface AtomicHandle {
  void push(OutPort out, DataItem<?> result);

  GroupingState<?> loadGroupingState();

  void saveGroupingState(GroupingState<?> storage);

  HashRange localRange();
}

