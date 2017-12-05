package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Grouping<T> extends Graph.Vertex.LocalTimeStub {
  private final HashFunction hash;
  private final BiPredicate<DataItem, DataItem> equalz;
  private final Class<T> clazz;
  private final int window;

  public Grouping(HashFunction hash, BiPredicate<DataItem, DataItem> equalz, int window, Class<T> clazz) {
    this.window = window;
    this.hash = hash;
    this.equalz = equalz;
    this.clazz = clazz;
  }

  public HashFunction hash() {
    return hash;
  }

  public BiPredicate<DataItem, DataItem> equalz() {
    return equalz;
  }

  public int window() {
    return window;
  }

  public BiFunction<DataItem, InvalidatingBucket, Stream<DataItem>> operation() {
    return (dataItem, bucket) -> {
      final int position = bucket.insert(dataItem);
      final Collection<DataItem> items = new ArrayList<>();
      for (int right = position + 1; right <= Math.min(position + window, bucket.size()); ++right) {
        final int left = Math.max(right - window, 0);
        final Meta meta = bucket.get(right - 1).meta().advanced(incrementLocalTimeAndGet());
        final List<T> groupingResult = bucket.rangeStream(left, right)
                .map(item -> item.payload(clazz))
                .collect(Collectors.toList());
        items.add(new PayloadDataItem(meta, groupingResult));
      }
      return items.stream();
    };
  }

  @Override
  public String toString() {
    return "Grouping{" +
            "hash=" + hash +
            ", equalz=" + equalz +
            ", window=" + window +
            '}';
  }
}
