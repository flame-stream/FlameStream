package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.AbstractAtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@SuppressWarnings({"rawtypes", "ConditionalExpression"})
public final class Grouping<T> extends AbstractAtomicGraph {
  private final InPort inPort;
  private final OutPort outPort = new OutPort();


  private final HashFunction<? super T> hash;
  private final int window;
  private final GroupingState<T> buffers;
  private GroupingState<T> state;


  public Grouping(final HashFunction<? super T> hash, final int window) {
    this.inPort = new InPort(hash);
    this.window = window;
    this.hash = hash;
    this.buffers = new LazyGroupingState<>(hash);
  }

  @Override
  public void onStart(final AtomicHandle handle) {
    this.state = new LazyGroupingState<>(this.hash);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    //noinspection unchecked
    final DataItem<T> dataItem = (DataItem<T>) item;
    List<DataItem<T>> group = this.buffers.get(dataItem).orElse(null);
    if (group != null) { // look for time collision in the current tick
      int replayCount = 0;
      while (replayCount < group.size() && group.get(group.size() - replayCount - 1).meta().compareTo(dataItem.meta()) > 0) {
        replayCount++;
      }
      group.add(group.size() - replayCount, dataItem);
      if (replayCount > 0) {
        for (int i = group.size() - replayCount; i < group.size(); i++) {
          handler.push(this.outPort(), new PayloadDataItem<GroupingResult>(
                  new Meta(group.get(i).meta(), this.incrementLocalTimeAndGet()),
                  new GroupingResult<>(group.subList(this.window > 0 ? Math.max(0, i + 1 - this.window) : 0, i + 1).stream().map(DataItem::payload).collect(Collectors.toList()), this.hash.applyAsInt(dataItem.payload()))));
        }
        return;
      }
    } else { // creating group from existing in the state
      group = new ArrayList<>(this.state.get(dataItem).orElse(Collections.emptyList()));
      group.add(dataItem);
      this.buffers.put(group);
    }
    handler.push(this.outPort(), new PayloadDataItem<GroupingResult>(
            new Meta(dataItem.meta(), this.incrementLocalTimeAndGet()),
            new GroupingResult<>(
                    (this.window > 0 ? group.subList(Math.max(0, group.size() - this.window), group.size()) : group).stream().map(DataItem::payload).collect(Collectors.toList()),
                    this.hash.applyAsInt(dataItem.payload()))));
  }

  @Override
  public void onCommit(final AtomicHandle handle) {
    this.buffers.forEach(group -> {
      final List<DataItem<T>> windowedGroup = group.subList(this.window > 0 ? Math.max(0, group.size() - this.window + 1) : 0, group.size());
      if (!windowedGroup.isEmpty()) {
        final List<DataItem<T>> oldGroup = this.state.get(group.get(0)).orElse(null);
        if (oldGroup != null) {
          oldGroup.clear();
          oldGroup.addAll(windowedGroup);
        } else {
          this.state.put(windowedGroup);
        }
      }
    });
    handle.saveGroupingState(this.state);
  }

  @Override
  public void onRecover(final GroupingState<?> state, final AtomicHandle handle) {
    //noinspection unchecked
    this.state = (GroupingState<T>) state;
  }

  @Override
  public void onMinGTimeUpdate(final Meta meta) {
    final Consumer<List<DataItem<T>>> removeOldConsumer = group -> {
      int removeIndex = 0;
      while (removeIndex < group.size() && group.get(group.size() - removeIndex - 1).meta().compareTo(meta) > 0) {
        removeIndex++;
      }
      group.subList(0, removeIndex).clear();
    };
    this.buffers.forEach(removeOldConsumer);
    this.state.forEach(removeOldConsumer);
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
