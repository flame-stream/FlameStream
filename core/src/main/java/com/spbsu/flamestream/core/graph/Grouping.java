package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

public class Grouping<T> extends HashingVertexStub {
  private final HashFunction hash;
  private final Equalz equalz;
  private final int window;
  private final Class<?> clazz;

  public Grouping(HashFunction hash, Equalz equalz, int window, Class<?> clazz) {
    this.window = window;
    this.hash = hash;
    this.equalz = equalz;
    this.clazz = clazz;
  }

  public HashFunction hash() {
    return hash;
  }

  public Equalz equalz() {
    return equalz;
  }

  public int window() {
    return window;
  }

  public GroupingOperation operation(long physicalId) {
    return new GroupingOperation(physicalId);
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
    private final long physicalId;

    GroupingOperation(long physicalId) {
      this.physicalId = physicalId;
    }

    public Stream<DataItem> apply(DataItem dataItem, InvalidatingBucket bucket) {
      final Collection<DataItem> items = new ArrayList<>();

      if (!dataItem.meta().isTombstone()) {
        final int positionToBeInserted = bucket.higherBound(dataItem.meta());
        items.addAll(replayAround(positionToBeInserted, bucket, true, false));
        bucket.insert(dataItem);
        items.addAll(replayAround(positionToBeInserted, bucket, false, true));
      } else {
        final int positionToBeCleared = bucket.higherBound(dataItem.meta()) - 1;
        items.addAll(replayAround(positionToBeCleared, bucket, true, true));
        bucket.insert(dataItem);
        items.addAll(replayAround(positionToBeCleared, bucket, false, false));
      }

      return items.stream();
    }

    private List<DataItem> replayAround(int index, InvalidatingBucket bucket, boolean areTombs, boolean include) {
      final List<DataItem> items = new ArrayList<>();
      for (int right = index + 1; right <= Math.min(index + window - (include ? 0 : 1), bucket.size()); ++right) {
        final int left = Math.max(right - window, 0);
        final List<T> groupingResult = new ArrayList<>();
        //noinspection unchecked
        bucket.forRange(left, right, dataItem -> groupingResult.add(dataItem.payload((Class<T>) clazz)));
        final Meta meta = new Meta(bucket.get(right - 1).meta(), physicalId, areTombs);
        items.add(new PayloadDataItem(meta, groupingResult));
      }
      return items;
    }
  }
}
