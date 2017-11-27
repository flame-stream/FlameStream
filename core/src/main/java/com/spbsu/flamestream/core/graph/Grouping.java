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

public class Grouping<T> extends Graph.Node.Stub implements BiFunction<DataItem<? extends T>, InvalidatingBucket, Stream<DataItem<List<T>>>> {
  private final HashFunction<? super T> hash;
  private final BiPredicate<? super T, ? super T> equalz;
  private final int window;


  public Grouping(HashFunction<? super T> hash, BiPredicate<? super T, ? super T> equalz, int window) {
    this.window = window;
    this.hash = hash;
    this.equalz = equalz;
  }

  @Override
  public HashFunction<? super T> inputHash() {
    return hash;
  }

  public BiPredicate<? super T, ? super T> equalz() {
    return this.equalz;
  }

  @Override
  public Stream<DataItem<List<T>>> apply(DataItem<? extends T> dataItem, InvalidatingBucket bucket) {
    final int position = bucket.insert(dataItem);
    final Collection<DataItem<List<T>>> items = new ArrayList<>();
    for (int right = position + 1; right <= Math.min(position + window, bucket.size()); ++right) {
      final int left = Math.max(right - window, 0);
      final Meta meta = bucket.get(right - 1).meta().advanced(incrementLocalTimeAndGet());
      //noinspection unchecked
      final List<T> groupingResult = bucket.rangeStream(left, right)
              .map(DataItem::payload)
              .map(o -> (T) o)
              .collect(Collectors.toList());
      items.add(new PayloadDataItem<>(meta, groupingResult));
    }
    return items.stream();
  }
}
