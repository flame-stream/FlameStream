package com.spbsu.flamestream.core.graph.ops;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.atomic.impl.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.atomic.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.graph.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.graph.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.graph.ops.stat.GroupingStatistics;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

public final class Grouping<T> extends AbstractAtomicGraph {
  private final GroupingStatistics stat = new GroupingStatistics();
  private final InPort inPort;
  private final OutPort outPort = new OutPort();

  private final ToIntFunction<? super T> hash;
  private final BiPredicate<? super T, ? super T> equalz;
  private final int window;

  private TIntObjectMap<Object> buffers = null;
  private GlobalTime currentMinTime = GlobalTime.MIN;

  public Grouping(ToIntFunction<? super T> hash, BiPredicate<? super T, ? super T> equalz, int window) {
    this.inPort = new InPort(hash);
    this.window = window;
    this.hash = hash;
    this.equalz = equalz;
  }

  @Override
  public void onCommit(AtomicHandle handle) {
    //handle.saveState(this.inPort, this.buffers);
    handle.submitStatistics(stat);
  }

  @Override
  public void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
    currentMinTime = globalTime;
  }

  public InPort inPort() {
    return inPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(inPort);
  }

  public OutPort outPort() {
    return outPort;
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(outPort);
  }

  @Override
  public void onStart(AtomicHandle handle) {
    // TODO: 5/18/17 Load state
    this.buffers = new TIntObjectHashMap<>();
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    //noinspection unchecked
    final DataItem<T> dataItem = (DataItem<T>) item;
    final InvalidatingBucket<T> bucket = getBucketFor(dataItem);
    final int position = bucket.insert(dataItem);
    stat.recordBucketSize(bucket.size());

    replayAround(position, bucket, handle);
    clearOutdated(bucket);
  }

  private void replayAround(int position, InvalidatingBucket<T> bucket, AtomicHandle handle) {
    int replayCount = 0;

    final List<DataItem<?>> items = new ArrayList<>();
    for (int right = position + 1; right <= Math.min(position + window, bucket.size()); ++right) {
      replayCount++;
      final int left = Math.max(right - window, 0);
      items.add(subgroup(bucket, left, right));
    }

    stat.recordReplaySize(replayCount);

    for (DataItem<?> item : items) {
      handle.push(outPort(), item);
      handle.ack(item.ack(), item.meta().globalTime());
    }
  }

  private DataItem<List<T>> subgroup(InvalidatingBucket<T> bucket, int left, int right) {
    final Meta meta = bucket.get(right - 1).meta().advanced(incrementLocalTimeAndGet());
    final List<T> groupingResult = bucket.rangeStream(left, right).map(DataItem::payload).collect(Collectors.toList());
    return new PayloadDataItem<>(meta, groupingResult);
  }

  private void clearOutdated(InvalidatingBucket<T> bucket) {
    final int position = Math.max(bucket.floor(Meta.meta(currentMinTime)) - window, 0);
    bucket.clearRange(0, position);
  }

  @SuppressWarnings("unchecked")
  private InvalidatingBucket<T> getBucketFor(DataItem<T> item) {
    final int hashValue = hash.applyAsInt(item.payload());
    final Object obj = buffers.get(hashValue);
    if (obj == null) {
      final InvalidatingBucket<T> newBucket = new ArrayInvalidatingBucket<>();
      buffers.put(hashValue, newBucket);
      return newBucket;
    } else {
      if (obj instanceof List) {
        final List<InvalidatingBucket<T>> container = (List<InvalidatingBucket<T>>) obj;
        final InvalidatingBucket<T> result = container.stream()
                .filter(bucket -> equalz.test(bucket.get(0).payload(), item.payload()))
                .findAny()
                .orElse(new ArrayInvalidatingBucket<>());
        if (result.isEmpty()) {
          container.add(result);
          return result;
        } else {
          return result;
        }
      } else {
        final InvalidatingBucket<T> bucket = (InvalidatingBucket<T>) obj;
        if (equalz.test(bucket.get(0).payload(), item.payload())) {
          return bucket;
        } else {
          final List<InvalidatingBucket<T>> container = new ArrayList<>();
          container.add(bucket);
          final InvalidatingBucket<T> newList = new ArrayInvalidatingBucket<>();
          container.add(newList);
          buffers.put(hash.applyAsInt(item.payload()), container);
          return newList;
        }
      }
    }
  }
}
