package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.state.GroupingState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Grouping<T> extends Graph.Node.Stub implements Function<DataItem<? extends T>, Stream<DataItem<List<T>>>> {
  private final HashFunction<? super T> hash;
  private final BiPredicate<? super T, ? super T> equalz;
  private final int window;
  private final GroupingState state;// TODO: 21.11.2017 create suitable interface for grouping state


  public Grouping(HashFunction<? super T> hash, BiPredicate<? super T, ? super T> equalz, int window, GroupingState state) {
    this.window = window;
    this.hash = hash;
    this.equalz = equalz;
    this.state = state;
  }

  @Override
  public HashFunction<? super T> inputHash() {
    return hash;
  }

  @Override
  public Stream<DataItem<List<T>>> apply(DataItem<? extends T> dataItem) {
    final InvalidatingBucket bucket = bucketFor(dataItem);
    final int position = bucket.insert(dataItem);
    final Collection<DataItem<List<T>>> items = new ArrayList<>();
    for (int right = position + 1; right <= Math.min(position + window, bucket.size()); ++right) {
      final int left = Math.max(right - window, 0);
      items.add(subgroup(bucket, left, right));
    }
    return items.stream();
  }

  private DataItem<List<T>> subgroup(InvalidatingBucket bucket, int left, int right) {
    final Meta meta = bucket.get(right - 1).meta().advanced(incrementLocalTimeAndGet());
    //noinspection unchecked
    final List<T> groupingResult = bucket.rangeStream(left, right)
            .map(DataItem::payload)
            .map(o -> (T) o)
            .collect(Collectors.toList());
    return new PayloadDataItem<>(meta, groupingResult);
  }

  private InvalidatingBucket bucketFor(DataItem<? extends T> item) {
    final int hashValue = hash.applyAsInt(item.payload());
    final Object obj = state.state().get(hashValue);
    if (obj == null) {
      final InvalidatingBucket newBucket = new ArrayInvalidatingBucket();
      state.state().put(hashValue, newBucket);
      return newBucket;
    } else {
      if (obj instanceof List) {
        //noinspection unchecked
        final List<InvalidatingBucket> container = (List<InvalidatingBucket>) obj;
        //noinspection unchecked
        final InvalidatingBucket result = container.stream()
                .filter(bucket -> equalz.test((T) bucket.get(0).payload(), item.payload()))
                .findAny()
                .orElse(new ArrayInvalidatingBucket());

        if (result.isEmpty()) {
          container.add(result);
        }
        return result;
      } else {
        final InvalidatingBucket bucket = (InvalidatingBucket) obj;
        //noinspection unchecked
        if (equalz.test((T) bucket.get(0).payload(), item.payload())) {
          return bucket;
        } else {
          final List<InvalidatingBucket> container = new ArrayList<>();
          container.add(bucket);
          final InvalidatingBucket newList = new ArrayInvalidatingBucket();
          container.add(newList);
          state.state().put(hash.applyAsInt(item.payload()), container);
          return newList;
        }
      }
    }
  }
}
