package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class GroupingJoba implements Joba {
  private final Grouping<?> grouping;
  private final Grouping<?>.GroupingOperation instance;
  private final TIntObjectMap<Object> buffers = new TIntObjectHashMap<>();

  private GlobalTime currentMinTime = GlobalTime.MIN;

  private final Tracing.Tracer tracer = Tracing.TRACING.forEvent("grouping-receive");

  public GroupingJoba(Grouping<?> grouping) {
    this.instance = grouping.operation(ThreadLocalRandom.current().nextLong());
    this.grouping = grouping;
  }

  @Override
  public void accept(DataItem item, Consumer<DataItem> sink) {
    tracer.log(item.xor());

    final InvalidatingBucket bucket = bucketFor(item);
    instance.apply(item, bucket).forEach(sink);
    { //clear outdated
      final int position = Math.max(bucket.lowerBound(new Meta(currentMinTime)) - grouping.window(), 0);
      bucket.clearRange(0, position);
    }

  }

  @Override
  public void onMinTime(GlobalTime globalTime) {
    currentMinTime = globalTime;
  }

  private InvalidatingBucket bucketFor(DataItem item) {
    final int hashValue = grouping.hash().applyAsInt(item);
    final Object obj = buffers.get(hashValue);
    if (obj == null) {
      final InvalidatingBucket newBucket = new ArrayInvalidatingBucket();
      buffers.put(hashValue, newBucket);
      return newBucket;
    } else {
      if (obj instanceof List) {
        //noinspection unchecked
        final List<InvalidatingBucket> container = (List<InvalidatingBucket>) obj;
        final InvalidatingBucket result = container.stream()
                .filter(bucket -> bucket.isEmpty() || grouping.equalz()
                        .test(bucket.get(0), item))
                .findAny()
                .orElse(new ArrayInvalidatingBucket());

        if (result.isEmpty()) {
          container.add(result);
        }
        return result;
      } else {
        final InvalidatingBucket bucket = (InvalidatingBucket) obj;
        if (bucket.isEmpty() || grouping.equalz().test(bucket.get(0), item)) {
          return bucket;
        } else {
          final List<InvalidatingBucket> container = new ArrayList<>();
          container.add(bucket);
          final InvalidatingBucket newList = new ArrayInvalidatingBucket();
          container.add(newList);
          buffers.put(grouping.hash().applyAsInt(item), container);
          return newList;
        }
      }
    }
  }
}
