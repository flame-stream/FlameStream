package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Meta;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;
import java.util.stream.Stream;

public class FlameMap<T, R> extends HashingVertexStub {
  private final Function<T, Stream<R>> function;
  private final Class<?> clazz;
  private final @Nullable HashFunction hashFunction;

  public FlameMap(Function<T, Stream<R>> function, Class<?> clazz, @Nullable HashFunction hashFunction) {
    this.function = function;
    this.clazz = clazz;
    this.hashFunction = hashFunction;
  }
  public FlameMap(Function<T, Stream<R>> function, Class<?> clazz) {
    this(function, clazz, null);
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

  @Override
  public @Nullable HashFunction hash() {
    return hashFunction;
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
        //System.out.format("FM.apply r %s%n", r);
        final Meta newMeta = new Meta(dataItem.meta(), physicalId, childId[0]++);
        //System.out.format("111111 from %s new meta %s%n", dataItem.meta(), newMeta);
        return new PayloadDataItem(newMeta, r);
      });
    }
  }
}
