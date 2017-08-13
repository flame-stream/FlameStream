package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.meta.Meta;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

@SuppressWarnings({"rawtypes", "ConditionalExpression"})
public final class Grouping<T> extends AbstractAtomicGraph {
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

  @Override
  public void onStart(AtomicHandle handle) {
    // TODO: 5/18/17 Load state
    this.buffers = new LazyGroupingState<>(this.hash, equalz);
  }

  @Override
  public void onPush(InPort inPort, DataItem<?> item, AtomicHandle handle) {
    //noinspection unchecked
    final DataItem<T> dataItem = (DataItem<T>) item;

    final List<DataItem<T>> group = this.buffers.getGroupFor(dataItem);
    final int position = this.insert(group, dataItem);
    this.replayAround(position, group, handle);
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

  private int insert(List<DataItem<T>> group, DataItem<T> insertee) {
    int position = 0;

    while (position < group.size()) {
      final DataItem<T> currentItem = group.get(position);
      if (insertee.meta().compareTo(currentItem.meta()) < 0) {
        break;
      } else if (insertee.meta().compareTo(currentItem.meta()) > 0) {
        if (currentItem.meta().isInvalidatedBy(insertee.meta())) {
          group.remove(position);
        } else {
          position++;
        }
      }
    }

    group.add(position, insertee);
    return position;
  }

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

      position = Math.max(position - window, 0);
      removeRange(group, position);
    };
    this.buffers.forEach(removeOldConsumer);
  }

  private <E> void removeRange(List<E> list, int toIndex) {
    for (int i = 0; i < toIndex; i++) {
      list.remove(0);
    }
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
