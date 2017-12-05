package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.function.Function;
import java.util.stream.Stream;

public class FlameMap<T, R> extends Graph.Vertex.LocalTimeStub {
  private final Function<T, Stream<R>> function;
  private final Class<T> clazz;

  public FlameMap(Function<T, Stream<R>> function, Class<T> clazz) {
    this.function = function;
    this.clazz = clazz;
  }

  public Function<DataItem, Stream<DataItem>> operation() {
    return dataItem -> {
      final Stream<R> result = function.apply(dataItem.payload(clazz));
      if (result == null) {
        return null;
      }

      final int newLocalTime = incrementLocalTimeAndGet();
      final int[] childId = {0};
      return result.map(r -> {
        final Meta newMeta = dataItem.meta().advanced(newLocalTime, childId[0]++);
        return new PayloadDataItem(newMeta, r);
      });
    };
  }

  @Override
  public String toString() {
    return "FlameMap{" +
            "function=" + function +
            '}';
  }
}
