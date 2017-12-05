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
public class GroupingJoba extends VertexJoba.SyncStub {
  private final Grouping<?> grouping;
  private final Consumer<DataItem> sink;
  private final TIntObjectMap<Object> buffers = new TIntObjectHashMap<>();

  private GlobalTime currentMinTime = GlobalTime.MIN;

  public GroupingJoba(Grouping<?> grouping, Consumer<DataItem> sink) {
    this.grouping = grouping;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem dataItem) {
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
  private InvalidatingBucket bucketFor(DataItem item) {
    final int hashValue = grouping.hash().applyAsInt(item.payload(grouping.clazz()));
    final Object obj = buffers.get(hashValue);
    if (obj == null) {
      final InvalidatingBucket newBucket = new ArrayInvalidatingBucket();
      buffers.put(hashValue, newBucket);
      return newBucket;
    } else {
      if (obj instanceof List) {
        final List<InvalidatingBucket> container = (List<InvalidatingBucket>) obj;
        final InvalidatingBucket result = container.stream()
                .filter(bucket -> grouping.equalz().test(bucket.get(0).payload(grouping.clazz()), item.payload(grouping.clazz())))
                .findAny()
                .orElse(new ArrayInvalidatingBucket());

        if (result.isEmpty()) {
          container.add(result);
        }
        return result;
      } else {
        final InvalidatingBucket bucket = (InvalidatingBucket) obj;
        if (grouping.equalz().test(bucket.get(0).payload(grouping.clazz()), item.payload(grouping.clazz()))) {
          return bucket;
        } else {
          final List<InvalidatingBucket> container = new ArrayList<>();
          container.add(bucket);
          final InvalidatingBucket newList = new ArrayInvalidatingBucket();
          container.add(newList);
          buffers.put(grouping.hash().applyAsInt(item.payload(grouping.clazz())), container);
          return newList;
        }
      }
    }
  }
}