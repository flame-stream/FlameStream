package com.spbsu.flamestream.runtime.graph.state;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.SynchronizedArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class GroupingState {
  private final ConcurrentMap<Integer, Object> buffers;

  public GroupingState() {
    buffers = new ConcurrentHashMap<>();
  }

  private GroupingState(ConcurrentMap<Integer, Object> buffers) {
    this.buffers = buffers;
  }

  public InvalidatingBucket bucketFor(DataItem item, HashFunction hash, Equalz equalz) {
    final int hashValue = hash.applyAsInt(item);
    final Object obj = buffers.get(hashValue);
    if (obj == null) {
      final InvalidatingBucket newBucket = new SynchronizedArrayInvalidatingBucket();
      buffers.put(hashValue, newBucket);
      return newBucket;
    } else {
      if (obj instanceof List) {
        //noinspection unchecked
        final List<InvalidatingBucket> container = (List<InvalidatingBucket>) obj;
        final InvalidatingBucket result = container.stream()
                .filter(bucket -> bucket.isEmpty() || equalz.test(bucket.get(0), item))
                .findAny()
                .orElse(new SynchronizedArrayInvalidatingBucket());

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
          final InvalidatingBucket newList = new SynchronizedArrayInvalidatingBucket();
          container.add(newList);
          buffers.put(hash.applyAsInt(item), container);
          return newList;
        }
      }
    }
  }

  public GroupingState subState(GlobalTime ceil, int window) {
    final ConcurrentMap<Integer, Object> subState = new ConcurrentHashMap<>();
    buffers.forEach((integer, obj) -> {
      if (obj instanceof List) {
        //noinspection unchecked
        subState.put(integer, ((List<InvalidatingBucket>) obj).stream()
                .map(bucket -> bucket.subBucket(new Meta(ceil), window))
                .collect(Collectors.toList()));
      } else {
        subState.put(integer, ((InvalidatingBucket) obj).subBucket(new Meta(ceil), window));
      }
    });
    return new GroupingState(subState);
  }
}
