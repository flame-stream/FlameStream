package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class Joba {
  public static class Id implements Comparable<Id> {
    final String nodeId;
    final String vertexId;
    private static final Comparator<Id> comparator = Comparator
            .<Id, String>comparing(id -> id.nodeId)
            .thenComparing(id -> id.vertexId);

    Id(String nodeId, String vertexId) {
      this.nodeId = nodeId;
      this.vertexId = vertexId;
    }

    @Override
    public int compareTo(Id that) {
      return comparator.compare(this, that);
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
