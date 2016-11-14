package com.spbsu.datastream.core.state;

import com.spbsu.datastream.core.DataItem;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.ObjLongConsumer;
import java.util.stream.Stream;

/**
 * Created by marnikitta on 14.11.16.
 */
public class GroupingState {

  private final TLongObjectHashMap<List<GroupingState.Bucket>> state = new TLongObjectHashMap<>();

  public Optional<Bucket> searchBucket(long hash, DataItem item, DataItem.Grouping grouping) {
    return Stream.of(state.get(hash))
            .flatMap(state -> state != null ? state.stream() : Stream.empty())
            .filter(bucket -> bucket.isEmpty() || grouping.equals(bucket.get(0), item)).findAny();
  }

  public void putBucket(long hash, Bucket bucket) {
    List<Bucket> buckets = Optional.ofNullable(state.get(hash)).orElse(new ArrayList<>());
    buckets.add(bucket);
    state.putIfAbsent(hash, buckets); //redundant
  }

  public void forEach(ObjLongConsumer<Bucket> consumer) {
    state.forEachEntry((hash, buckets) -> {
      buckets.forEach(bucket -> {
        consumer.accept(bucket, hash);
      });
      return true;
    });
  }

  public static class Bucket extends ArrayList<DataItem> implements Cloneable {
    public Bucket(final int initialCapacity) {
      super(initialCapacity);
    }

    public Bucket() {
    }

    public Bucket(final Collection<? extends DataItem> c) {
      super(c);
    }

    public Bucket clone() {
      return new Bucket(this);
    }
  }
}
