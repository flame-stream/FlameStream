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

  public FlameMap(Function<T, Stream<R>> function, Class<?> clazz) {
    this.function = function;
    this.clazz = clazz;
  }

  public FlameMapOperation operation(long physicalId) {
    return new FlameMapOperation(physicalId);
  }

  @Override
  public String toString() {
    return "FlameMap{" +
            "function=" + function +
            '}';
  }

  public Function<T, Stream<R>> function() {
    return function;
  }

  public class FlameMapOperation {
    private final long physicalId;

    FlameMapOperation(long physicalId) {
      this.physicalId = physicalId;
    }

    public Stream<DataItem> apply(DataItem dataItem) {
      //noinspection unchecked
      final Stream<R> result = function.apply(dataItem.payload((Class<T>) clazz));
      final int[] childId = {0};
      return result.map(r -> {
        final Meta newMeta = new Meta(dataItem.meta(), physicalId, childId[0]++);
        return new PayloadDataItem(newMeta, r);
      });
    }
  }
}
