package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public class BroadcastJoba<T> extends VertexJoba.SyncStub<T> {
  private static final int BROADCAST_LOCAL_TIME = Integer.MAX_VALUE;
  private final List<Consumer<DataItem<T>>> sinks = new ArrayList<>();

  @Override
  public void accept(DataItem<T> dataItem) {
    for (int i = 0; i < sinks.size(); ++i) {
      final Meta newMeta = dataItem.meta().advanced(BROADCAST_LOCAL_TIME, i);
      final DataItem<T> newItem = new PayloadDataItem<>(newMeta, dataItem.payload());
      sinks.get(i).accept(newItem);
    }
  }

  public void addSink(Consumer<DataItem<T>> sink) {
    sinks.add(sink);
  }
}
