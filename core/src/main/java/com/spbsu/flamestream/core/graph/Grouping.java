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

      //System.out.format("GO apply: %d %b %s%n", bucket.size(), dataItem.meta().isTombstone(), dataItem);
      //for (int i = 0; i < bucket.size(); i++) {
      //  System.out.format("III %d/%d   %s%n", i, bucket.size(), bucket.get(i));
      //}

      if (!dataItem.meta().isTombstone()) {
        final int positionToBeInserted = bucket.lowerBound(dataItem.meta());
        //System.out.println("1 ITEMS pos: " + positionToBeInserted);

        items.addAll(replayAround(positionToBeInserted, bucket, true, false));
        //System.out.println("1 ITEMS-1: " + items);
        bucket.insert(dataItem);
        items.addAll(replayAround(positionToBeInserted, bucket, false, true));
      } else {
        final int positionToBeCleared = bucket.lowerBound(dataItem.meta()) - 1;
        //System.out.println("2 ITEMS pos: " + positionToBeCleared);
        items.addAll(replayAround(positionToBeCleared, bucket, true, true));
        //System.out.println("2 ITEMS-1: " + items);
        bucket.insert(dataItem);
        items.addAll(replayAround(positionToBeCleared, bucket, false, false));
      }
      //System.out.format(">>> GO apply: %d %b %s%n", bucket.size(), dataItem.meta().isTombstone(), dataItem);
      //for (int i = 0; i < bucket.size(); i++) {
      //  System.out.format(">>> III %d/%d   %s%n", i, bucket.size(), bucket.get(i));
      //}

      //System.out.format("GO apply result: %d %s%n", bucket.size(), items);

      // System.out.println("ITEMS: " + items);

      return items.stream();
    }

    private List<DataItem> replayAround(int index, InvalidatingBucket bucket, boolean areTombs, boolean include) {
      final List<DataItem> items = new ArrayList<>();
      //System.out.println("");
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
