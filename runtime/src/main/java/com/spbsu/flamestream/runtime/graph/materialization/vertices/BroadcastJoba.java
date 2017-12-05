package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public class BroadcastJoba implements VertexJoba {
  private static final int BROADCAST_LOCAL_TIME = Integer.MAX_VALUE;
  private final List<Consumer<DataItem>> sinks = new ArrayList<>();

  @Override
  public void accept(DataItem dataItem) {
    for (int i = 0; i < sinks.size(); ++i) {
      final Meta newMeta = dataItem.meta().advanced(BROADCAST_LOCAL_TIME, i);
      final DataItem newItem = new BroadcastDataItem(dataItem, newMeta);
      sinks.get(i).accept(newItem);
    }
  }

  private static class BroadcastDataItem implements DataItem {
    private final DataItem inner;
    private final Meta newMeta;

    private BroadcastDataItem(DataItem inner, Meta newMeta) {
      this.inner = inner;
      this.newMeta = newMeta;
    }

    @Override
    public Meta meta() {
      return newMeta;
    }

    @Override
    public <T> T payload(Class<T> expectedClass) {
      return inner.payload(expectedClass);
    }

    @Override
    public long xor() {
      return inner.xor();
    }
  }

  public void addSink(Consumer<DataItem> sink) {
    sinks.add(sink);
  }
}
