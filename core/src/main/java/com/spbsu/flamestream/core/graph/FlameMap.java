package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.function.Function;
import java.util.stream.Stream;

public class FlameMap<T, R> extends Graph.Node.Stub<T> implements Function<DataItem<? extends T>, Stream<DataItem<R>>> {
  private final Function<T, Stream<R>> function;
  private final HashFunction<? super T> hash;

  public FlameMap(Function<T, Stream<R>> function, HashFunction<? super T> hash) {
    this.function = function;
    this.hash = hash;
  }

  @Override
  public HashFunction<? super T> inputHash() {
    return hash;
  }

  @Override
  public Stream<DataItem<R>> apply(DataItem<? extends T> dataItem) {
    final Stream<R> result = function.apply(dataItem.payload());
    if (result == null) {
      return null;
    }

    final int newLocalTime = incrementLocalTimeAndGet();
    final int[] childId = {0};
    return result.map(r -> {
      final Meta newMeta = dataItem.meta().advanced(newLocalTime, childId[0]++);
      return new PayloadDataItem<>(newMeta, r);
    });
  }
}
