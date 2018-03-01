package com.spbsu.flamestream.runtime.graph.state;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class GroupingState {
  private final ConcurrentMap<Integer, Object> buffers = new ConcurrentHashMap<>();

  private final HashFunction hash;
  private final Equalz equalz;

  public GroupingState(HashFunction hash, Equalz equalz) {
    this.hash = hash;
    this.equalz = equalz;
  }

  public InvalidatingBucket bucketFor(DataItem item) {
    final int hashValue = hash.applyAsInt(item);
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
                .filter(bucket -> bucket.isEmpty() || equalz.test(bucket.get(0), item))
                .findAny()
                .orElse(new ArrayInvalidatingBucket());

        if (result.isEmpty()) {
          container.add(result);
        }
        return result;
      } else {
        final InvalidatingBucket bucket = (InvalidatingBucket) obj;
        if (bucket.isEmpty() || equalz.test(bucket.get(0), item)) {
          return bucket;
        } else {
          final List<InvalidatingBucket> container = new CopyOnWriteArrayList<>();
          container.add(bucket);
          final InvalidatingBucket newList = new ArrayInvalidatingBucket();
          container.add(newList);
          buffers.put(hash.applyAsInt(item), container);
          return newList;
        }
      }
    }
  }

  public void forEachBucket(Consumer<InvalidatingBucket> consumer) {
    buffers.forEach((integer, obj) -> {
      if (obj instanceof List) {
        //noinspection unchecked
        final List<InvalidatingBucket> container = (List<InvalidatingBucket>) obj;
        container.forEach(consumer);
      } else {
        consumer.accept((InvalidatingBucket) obj);
      }
    });
  }
}
