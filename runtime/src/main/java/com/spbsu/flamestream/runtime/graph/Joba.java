package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

public abstract class Joba {
  public static class Id {
    final String nodeId;
    final String vertexId;

    Id(String nodeId, String vertexId) {
      this.nodeId = nodeId;
      this.vertexId = vertexId;
    }
  }
  final Id id;

  Joba(Id id) {
    this.id = id;
  }
  public long time = Long.MIN_VALUE;
  abstract void accept(DataItem item, Consumer<DataItem> sink);

  void onMinTime(GlobalTime time) {
  }

  void onPrepareCommit(GlobalTime time) {
  }
}
