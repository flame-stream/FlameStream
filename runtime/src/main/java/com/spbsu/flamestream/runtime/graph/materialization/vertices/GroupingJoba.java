package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.Grouping;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class GroupingJoba<T> extends VertexJoba.Stub<T> {
  private final Grouping<T> grouping;
  private final Consumer<DataItem<List<T>>> sink;
  private final TIntObjectMap<Object> buffers = new TIntObjectHashMap<>();

  private GlobalTime currentMinTime = GlobalTime.MIN;

  public GroupingJoba(Grouping<T> grouping, Consumer<DataItem<List<T>>> sink) {
    this.grouping = grouping;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem<T> dataItem) {
    final InvalidatingBucket bucket = bucketFor(dataItem);
    grouping.operation().apply(dataItem, bucket).forEach(sink);
    { //clear outdated
      final int position = Math.max(bucket.floor(Meta.meta(currentMinTime)) - grouping.window(), 0);
      bucket.clearRange(0, position);
    }
  }

  @Override
  public void onMinTime(GlobalTime globalTime) {
    currentMinTime = globalTime;
  }

  @SuppressWarnings("unchecked")
  private InvalidatingBucket bucketFor(DataItem<T> item) {
    final int hashValue = grouping.hash().applyAsInt(item.payload());
    final Object obj = buffers.get(hashValue);
    if (obj == null) {
      final InvalidatingBucket newBucket = new ArrayInvalidatingBucket();
      buffers.put(hashValue, newBucket);
      return newBucket;
    } else {
      if (obj instanceof List) {
        final List<InvalidatingBucket> container = (List<InvalidatingBucket>) obj;
        final InvalidatingBucket result = container.stream()
                .filter(bucket -> grouping.equalz().test((T) bucket.get(0).payload(), item.payload()))
                .findAny()
                .orElse(new ArrayInvalidatingBucket());

        if (result.isEmpty()) {
          container.add(result);
        }
        return result;
      } else {
        final InvalidatingBucket bucket = (InvalidatingBucket) obj;
        if (grouping.equalz().test((T) bucket.get(0).payload(), item.payload())) {
          return bucket;
        } else {
          final List<InvalidatingBucket> container = new ArrayList<>();
          container.add(bucket);
          final InvalidatingBucket newList = new ArrayInvalidatingBucket();
          container.add(newList);
          buffers.put(grouping.hash().applyAsInt(item.payload()), container);
          return newList;
        }
      }
    }
  }
}
