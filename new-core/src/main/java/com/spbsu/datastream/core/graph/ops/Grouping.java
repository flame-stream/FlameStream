package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.LocalEvent;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.Trace;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
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
      this.replayAround(group, position, handle);
    }
  }

  private void replayAround(List<DataItem<T>> group, int position, AtomicHandle handle) {
    for (int right = position + 1; right <= Math.min(position + this.window, group.size()); ++right) {
      final int left = Math.max(right - this.window, 0);
      this.play(group, left, right, handle);
    }
  }

  private void play(List<DataItem<T>> group, int left, int right, AtomicHandle handle) {
    final List<DataItem<T>> outGroup = group.subList(left, right);

    final Meta meta = new Meta(outGroup.get(outGroup.size() - 1).meta(), this.incrementLocalTimeAndGet());
    final int hashValue = this.hash.applyAsInt(group.get(0).payload());
    final GroupingResult<T> groupingResult = new GroupingResult<>(outGroup.stream().map(DataItem::payload).collect(Collectors.toList()), hashValue);

    final DataItem<GroupingResult<T>> result = new PayloadDataItem<>(meta, groupingResult);
    handle.push(this.outPort(), result);
  }

  private int insert(List<DataItem<T>> group, DataItem<T> item) {
    final int position = Collections.binarySearch(group, item, this.itemComparator);
    if (position >= 0) {
      final int invalidationRelation = this.itemInvalidationComparator.compare(item, group.get(position));
      if (invalidationRelation > 0) {
        group.set(position, item);
        return position;
      } else {
        return -1;
      }
    } else {
      group.add(-(position + 1), item);
      return -(position + 1);
    }
  }


  public static final Comparator<Trace> INVALIDATION_COMPARATOR = (t0, t1) -> {
    for (int i = 0; i < Math.min(t0.size(), t1.size()); ++i) {
      if (!t0.eventAt(i).equals(t1.eventAt(i))) {
        return Long.compare(t0.eventAt(i).localTime(), t1.eventAt(i).localTime());
      }
    }
    return 0;
  };

  private static final Comparator<Trace> INVALIDATION_IGNORING_COMPARATOR = (t0, t1) -> {
    for (int i = 0; i < Math.min(t0.size(), t1.size()); ++i) {
      final LocalEvent t0Event = t0.eventAt(i);
      final LocalEvent t1Event = t1.eventAt(i);
      if (!Objects.equals(t0Event, t1Event)) {
        if (t0Event.localTime() == t1Event.localTime()) {
          return Integer.compare(t0Event.childId(), t1Event.childId());
        } else {
          return 0;
        }
      }
    }
    return Integer.compare(t0.size(), t1.size());
  };

  private static final Comparator<DataItem<?>> itemComparator = Comparator
          .comparing((DataItem<?> di) -> di.meta().globalTime())
          .thenComparing((DataItem<?> di) -> di.meta().trace(),
                  Grouping.INVALIDATION_IGNORING_COMPARATOR);

  private static final Comparator<DataItem<?>> itemInvalidationComparator = Comparator
          .comparing((DataItem<?> di) -> di.meta().trace(), Grouping.INVALIDATION_COMPARATOR);


  @Override
  public void onCommit(AtomicHandle handle) {
    handle.saveState(this.inPort, this.buffers);
  }

  @Override
  public void onMinGTimeUpdate(GlobalTime globalTime, AtomicHandle handle) {
    // TODO: 5/18/17 COMPACT
    //final Consumer<List<DataItem<T>>> removeOldConsumer = group -> {
    //  int removeIndex = 0;
    //  while (removeIndex < group.size() && metaComparator.compare(group.get(group.size() - removeIndex - 1).meta(), meta) > 0) {
    //    removeIndex++;
    //  }
    //  group.subList(0, removeIndex).clear();
    //};
    //this.buffers.forEach(removeOldConsumer);
    //this.state.forEach(removeOldConsumer);
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
