package com.spbsu.flamestream.core.graph.ops;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.graph.invalidation.InvalidatingList;
import com.spbsu.flamestream.core.graph.invalidation.impl.ArrayInvalidatingList;
import com.spbsu.flamestream.core.graph.ops.stat.GroupingStatistics;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

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

  private TLongObjectMap<Object> buffers = null;
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
    this.buffers = new TLongObjectHashMap<>();
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    //noinspection unchecked
    final DataItem<T> dataItem = (DataItem<T>) item;
    final InvalidatingList<T> group = getGroupFor(dataItem);
    final int position = group.insert(dataItem);
    stat.recordBucketSize(group.size());

    replayAround(position, group, handle);
    clearOutdated(group);
  }

  private void replayAround(int position, List<DataItem<T>> group, AtomicHandle handle) {
    int replayCount = 0;

    final List<DataItem<?>> items = new ArrayList<>();
    for (int right = position + 1; right <= Math.min(position + window, group.size()); ++right) {
      replayCount++;
      final int left = Math.max(right - window, 0);
      items.add(subgroup(group, left, right));
    }

    stat.recordReplaySize(replayCount);

    for (DataItem<?> item : items) {
      handle.push(outPort(), item);
      handle.ack(item.ack(), item.meta().globalTime());
    }
  }

  private DataItem<List<T>> subgroup(List<DataItem<T>> group, int left, int right) {
    final List<DataItem<T>> outGroup = group.subList(left, right);

    final Meta meta = outGroup.get(outGroup.size() - 1).meta().advanced(incrementLocalTimeAndGet());
    final List<T> groupingResult = outGroup.stream().map(DataItem::payload).collect(Collectors.toList());

    return new PayloadDataItem<>(meta, groupingResult);
  }

  private void clearOutdated(List<DataItem<T>> group) {
    int left = 0;
    int right = group.size();
    { //upper-bound binary search
      while (right - left > 1) {
        final int middle = left + (right - left) / 2;
        if (group.get(middle).meta().globalTime().compareTo(currentMinTime) <= 0) {
          left = middle;
        } else {
          right = middle;
        }
      }
    }

    final int position = left - window + 1;
    if (position > 0) {
      group.subList(0, position).clear();
    }
  }

  @SuppressWarnings("unchecked")
  private InvalidatingList<T> getGroupFor(DataItem<T> item) {
    final long hashValue = hash.applyAsInt(item.payload());
    final Object obj = buffers.get(hashValue);
    if (obj == null) {
      final InvalidatingList<T> newBucket = new ArrayInvalidatingList<>();
      buffers.put(hashValue, newBucket);
      return newBucket;
    } else {
      final List<?> list = (List<?>) obj;
      if (list.get(0) instanceof List) {
        final List<InvalidatingList<T>> container = (List<InvalidatingList<T>>) list;
        return getFromContainer(item, container);
      } else {
        final InvalidatingList<T> bucket = (InvalidatingList<T>) list;
        return getFromBucket(item, bucket);
      }
    }
  }

  private InvalidatingList<T> getFromContainer(DataItem<T> item, List<InvalidatingList<T>> container) {
    final InvalidatingList<T> result = searchBucket(item, container);
    if (result.isEmpty()) {
      container.add(result);
      return result;
    } else {
      return result;
    }
  }

  private InvalidatingList<T> getFromBucket(DataItem<T> item, InvalidatingList<T> bucket) {
    if (equalz.test(bucket.get(0).payload(), item.payload())) {
      return bucket;
    } else {
      final List<InvalidatingList<T>> container = new ArrayList<>();
      container.add(bucket);
      final InvalidatingList<T> newList = new ArrayInvalidatingList<>();
      container.add(newList);
      buffers.put(hash.applyAsInt(item.payload()), container);
      return newList;
    }
  }

  private InvalidatingList<T> searchBucket(DataItem<T> item, List<InvalidatingList<T>> container) {
    return container.stream()
            .filter(bucket -> equalz.test(bucket.get(0).payload(), item.payload()))
            .findAny()
            .orElse(new ArrayInvalidatingList<>());
  }
}
