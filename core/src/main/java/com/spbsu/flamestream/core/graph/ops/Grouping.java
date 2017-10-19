package com.spbsu.flamestream.core.graph.ops;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.AbstractAtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.graph.ops.stat.GroupingStatistics;
import com.spbsu.flamestream.core.graph.ops.state.GroupingState;
import com.spbsu.flamestream.core.graph.ops.state.LazyGroupingState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

@SuppressWarnings({"ConditionalExpression"})
public final class Grouping<T> extends AbstractAtomicGraph {
  //private static final int MIN_BUFFER_SIZE_FOR_MIN_TIME_UPDATE = 200; //magic number, tuning is welcome
  private final GroupingStatistics stat = new GroupingStatistics();

  private final InPort inPort;
  private final OutPort outPort = new OutPort();

  private final ToIntFunction<? super T> hash;
  private final BiPredicate<? super T, ? super T> equalz;
  private final int window;
  private GroupingState<T> buffers;

  public Grouping(ToIntFunction<? super T> hash, BiPredicate<? super T, ? super T> equalz, int window) {
    this.inPort = new InPort(hash);
    this.window = window;
    this.hash = hash;
    this.equalz = equalz;
  }

  public static <T> int insert(List<DataItem<T>> group, DataItem<T> insertee) {
    int position = group.size() - 1;
    int endPosition = -1;
    { //find position
      while (position >= 0) {
        final DataItem<T> currentItem = group.get(position);
        final int compareTo = currentItem.meta().compareTo(insertee.meta());

        if (compareTo > 0) {
          if (insertee.meta().isInvalidatedBy(currentItem.meta())) {
            return -1;
          }
          position--;
        } else {
          if (currentItem.meta().isInvalidatedBy(insertee.meta())) {
            endPosition = endPosition == -1 ? position : endPosition;
            position--;
          } else {
            break;
          }
        }
      }
    }
    { //invalidation/adding
      if (position == (group.size() - 1)) {
        group.add(insertee);
      } else {
        if (endPosition != -1) {
          group.set(position + 1, insertee);
          final int itemsForRemove = endPosition - position - 1;
          //subList.clear is faster if the number of items for removing >= 2
          if (itemsForRemove >= 2)
            group.subList(position + 2, endPosition + 1).clear();
          else if (itemsForRemove > 0)
            group.remove(endPosition);
        } else {
          group.add(position + 1, insertee);
        }
      }
    }
    return position + 1;
  }

  @Override
  public void onStart(AtomicHandle handle) {
    // TODO: 5/18/17 Load state
    this.buffers = new LazyGroupingState<>(hash, equalz);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    //noinspection unchecked
    final DataItem<T> dataItem = (DataItem<T>) item;

    final List<DataItem<T>> group = buffers.getGroupFor(dataItem);
    final int position = insert(group, dataItem);
    stat.recordBucketSize(group.size());
    replayAround(position, group, handle);
  }

  private void replayAround(int position, List<DataItem<T>> group, AtomicHandle handle) {
    int replayCount = 0;

    final List<DataItem<?>> items = new ArrayList<>();
    for (int right = position + 1; right <= Math.min(position + window, group.size()); ++right) {
      replayCount++;
      final int left = Math.max(right - window, 0);
      items.add(subgroup(group, left, right, handle));
    }

    stat.recordReplaySize(replayCount);

    for (DataItem<?> item : items) {
      handle.push(outPort(), item);
      handle.ack(item.ack(), item.meta().globalTime());
    }
  }

  private DataItem<List<T>> subgroup(List<DataItem<T>> group, int left, int right, AtomicHandle handle) {
    final List<DataItem<T>> outGroup = group.subList(left, right);

    final Meta meta = outGroup.get(outGroup.size() - 1).meta().advanced(incrementLocalTimeAndGet());
    final List<T> groupingResult = outGroup.stream().map(DataItem::payload).collect(Collectors.toList());

    return new PayloadDataItem<>(meta, groupingResult);
  }

  @Override
  public void onCommit(AtomicHandle handle) {
    //handle.saveState(this.inPort, this.buffers);
    handle.submitStatistics(stat);
  }

  @Override
  public void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
    // TODO: 29.09.2017 optimize or remove
    /*final Consumer<List<DataItem<T>>> removeOldConsumer = group -> {
      if (group.size() < MIN_BUFFER_SIZE_FOR_MIN_TIME_UPDATE)
        return;

      int left = 0;
      int right = group.size();
      { //upper-bound binary search
        while (right - left > 1) {
          final int middle = left + (right - left) / 2;
          if (group.get(middle).meta().globalTime().compareTo(globalTime) <= 0) {
            left = middle;
          } else {
            right = middle;
          }
        }
      }

      final int position = Math.max(left - window, 0);
      if (position > 0) {
        group.subList(0, position).clear();
      }
    };
    buffers.forEach(removeOldConsumer);*/
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
}
