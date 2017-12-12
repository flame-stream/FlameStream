package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.function.Function;
import java.util.stream.Stream;

public class FlameMap<T, R> extends Graph.Vertex.Stub {
  private final Function<T, Stream<R>> function;
  private final Class<?> clazz;

  private final FlameMapOperation flameMapOperation = new FlameMapOperation();

  public FlameMap(Function<T, Stream<R>> function, Class<?> clazz) {
    this.function = function;
    this.clazz = clazz;
  }

  public FlameMapOperation operation() {
    return flameMapOperation;
  }

  @Override
  public String toString() {
    return "FlameMap{" +
            "function=" + function +
            '}';
  }

  public class FlameMapOperation {
    public Stream<DataItem> apply(DataItem dataItem, int localTime) {
      final Stream<R> result = function.apply(dataItem.payload((Class<T>) clazz));
      final int[] childId = {0};
      return result.map(r -> {
        final Meta newMeta = new Meta(dataItem.meta(), localTime, childId[0]++);
        return new PayloadDataItem(newMeta, r);
      });
    }
  }
}
