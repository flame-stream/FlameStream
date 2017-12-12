package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Grouping<T> extends Graph.Vertex.Stub {
  private final HashFunction hash;
  private final Equalz equalz;
  private final int window;
  private final Class<?> clazz;

  private final GroupingOperation groupingOperation = new GroupingOperation();

  public Grouping(HashFunction hash, Equalz equalz, int window, Class<?> clazz) {
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

  public GroupingOperation operation() {
    return groupingOperation;
  }

  @Override
  public String toString() {
    return "Grouping{" +
            "hash=" + hash +
            ", equalz=" + equalz +
            ", window=" + window +
            '}';
  }

  public class GroupingOperation {
    public Stream<DataItem> apply(DataItem dataItem, InvalidatingBucket bucket, int localTime) {
      final int position = bucket.insert(dataItem);
      final Collection<DataItem> items = new ArrayList<>();
      for (int right = position + 1; right <= Math.min(position + window, bucket.size()); ++right) {
        final int left = Math.max(right - window, 0);
        final Meta meta = Meta.advanced(bucket.get(right - 1).meta(), localTime);
        final List<T> groupingResult = bucket.rangeStream(left, right)
                .map(item -> item.payload((Class<T>) clazz))
                .collect(Collectors.toList());
        items.add(new PayloadDataItem(meta, groupingResult));
      }
      return items.stream();
    }
  }
}
