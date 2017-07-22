package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.Trace;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@SuppressWarnings({"rawtypes", "ConditionalExpression"})
public final class Grouping<T> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final OutPort outPort = new OutPort();

  private final HashFunction<? super T> hash;
  private final int window;
  private GroupingState<T> buffers;

  public Grouping(HashFunction<? super T> hash, int window) {
    this.inPort = new InPort(hash);
    this.window = window;
    this.hash = hash;
  }

  @Override
  public void onStart(AtomicHandle handle) {
    // TODO: 5/18/17 Load state
    this.buffers = new LazyGroupingState<>(this.hash);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    //noinspection unchecked
    final DataItem<T> dataItem = (DataItem<T>) item;

    final List<DataItem<T>> group = this.buffers.getGroupFor(dataItem);
    final int position = this.insert(group, dataItem);
    if (position != -1) {
      this.replayAround(position, group, handle);
    }
  }

  private void replayAround(int position, List<DataItem<T>> group, AtomicHandle handle) {
    for (int right = position + 1; right <= Math.min(position + this.window, group.size()); ++right) {
      final int left = Math.max(right - this.window, 0);
      this.pushSubGroup(group, left, right, handle);
    }
  }

  private void pushSubGroup(List<DataItem<T>> group, int left, int right, AtomicHandle handle) {
    final List<DataItem<T>> outGroup = group.subList(left, right);

    final Meta meta = outGroup.get(outGroup.size() - 1).meta().advanced(this.incrementLocalTimeAndGet());
    final List<T> groupingResult = outGroup.stream().map(DataItem::payload).collect(Collectors.toList());

    final DataItem<List<T>> result = new PayloadDataItem<>(meta, groupingResult);
    handle.push(this.outPort(), result);
  }

  private int insert(List<DataItem<T>> group, DataItem<T> item) {
    final int position = Collections.binarySearch(group, item, Grouping.ITEM_COMPARATOR);
    if (position >= 0) {
      final DataItem<T> olderItem = group.get(position);
      final int invalidationRelation = Grouping.ITEM_INVALIDATION_COMPARATOR.compare(item, olderItem);
      if (invalidationRelation > 0) {
        int leftmostBrother = position;
        while ((leftmostBrother - 1) >= 0 && group.get(leftmostBrother - 1).meta().isBrother(olderItem.meta()))
          leftmostBrother--;
        int rightmostBrother = position;
        while ((rightmostBrother + 1) < group.size() && group.get(rightmostBrother + 1).meta().isBrother(olderItem.meta()))
          rightmostBrother++;
        if (rightmostBrother - leftmostBrother > 0)
          group.subList(leftmostBrother + 1, rightmostBrother + 1).clear();
        group.set(leftmostBrother, item);
        return leftmostBrother;
      } else {
        return -1;
      }
    } else {
      group.add(-(position + 1), item);
      return -(position + 1);
    }
  }

  private static final Comparator<DataItem<?>> ITEM_COMPARATOR = Comparator
          .comparing((DataItem<?> di) -> di.meta().globalTime())
          .thenComparing((DataItem<?> di) -> di.meta().trace(),
                  Trace.INVALIDATION_IGNORING_COMPARATOR);

  private static final Comparator<DataItem<?>> ITEM_INVALIDATION_COMPARATOR = Comparator
          .comparing((DataItem<?> di) -> di.meta().trace(), Trace.INVALIDATION_COMPARATOR);

  @Override
  public void onCommit(AtomicHandle handle) {
    //handle.saveState(this.inPort, this.buffers);
  }

  @Override
  public void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
    final Consumer<List<DataItem<T>>> removeOldConsumer = group -> {
      int position = 0;
      while (position < group.size()
              && group.get(position).meta().globalTime().compareTo(globalTime) < 0) {
        position++;
      }

      //  [position, size) - to keep. But we need to add window to position: [position - window, size).
      // And do not forget about brothers, that can be invalidated at once.

      int groupSize = 0;
      Meta previousMeta = null;
      while (groupSize <  this.window && position - 1 >= 0) {

        if (previousMeta == null || !group.get(position - 1).meta().isBrother(previousMeta)) {
          groupSize++;
        }

        previousMeta = group.get(position - 1).meta();
        position--;
      }

      final List<DataItem<T>> toKeep = new ArrayList<>(group.subList(position, group.size()));
      group.clear();
      group.addAll(toKeep);
    };
    this.buffers.forEach(removeOldConsumer);
  }

  public InPort inPort() {
    return this.inPort;
  }

  @Override
  public List<InPort> inPorts() {
    return Collections.singletonList(this.inPort);
  }

  public OutPort outPort() {
    return this.outPort;
  }

  @Override
  public List<OutPort> outPorts() {
    return Collections.singletonList(this.outPort);
  }
}
