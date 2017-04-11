package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.tick.atomic.AtomicHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Grouping<T> extends Processor<T, GroupingResult<? super T>> {
  private final int window;
  private final HashFunction<T> hash;
  private final GroupingState<T> buffers;
  private GroupingState<T> state;

  public Grouping(final HashFunction<T> hash, final int window) {
    super(hash);
    this.hash = hash;
    this.window = window;
    buffers = new LazyGroupingState<>(hash);
  }

  @Override
  public void onStart(final AtomicHandle handle) {
    state = new LazyGroupingState<>(hash);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    //noinspection unchecked
    final DataItem<T> dataItem = (DataItem<T>) item;
    List<DataItem<T>> group = buffers.get(dataItem).orElse(null);
    if (group != null) { // look for time collision in the current tick
      int replayCount = 0;
      //noinspection unchecked
      while (replayCount < group.size() && group.get(group.size() - replayCount - 1).meta().compareTo(dataItem.meta()) > 0) {
        replayCount++;
      }
      group.add(group.size() - replayCount, dataItem);
      if (replayCount > 0) {
        for (int i = group.size() - replayCount; i < group.size(); i++) {
          prePush(dataItem, handler);
          handler.push(outPort(), new PayloadDataItem<GroupingResult>(
                  group.get(i).meta(),
                  new GroupingResult<>(group.subList(window > 0 ? Math.max(0, i + 1 - window) : 0, i + 1).stream().map(DataItem::payload).collect(Collectors.toList()), hash.applyAsInt(dataItem.payload()))));
          ack(dataItem, handler);
        }
        return;
      }
    } else { // creating group from existing in the state
      group = new ArrayList<>(state.get(dataItem).orElse(Collections.emptyList()));
      group.add(dataItem);
      buffers.put(group);
    }
    prePush(dataItem, handler);
    handler.push(outPort(), new PayloadDataItem<GroupingResult>(
            dataItem.meta(),
            new GroupingResult<>(
                    (window > 0 ? group.subList(Math.max(0, group.size() - window), group.size()) : group).stream().map(DataItem::payload).collect(Collectors.toList()),
                    hash.applyAsInt(dataItem.payload()))));
    ack(dataItem, handler);
  }

  @Override
  public void onCommit(final AtomicHandle handle) {
    buffers.forEach(group -> {
      final List<DataItem<T>> windowedGroup = group.subList(window > 0 ? Math.max(0, group.size() - window + 1) : 0, group.size());
      if (!windowedGroup.isEmpty()) {
        final List<DataItem<T>> oldGroup = state.get(group.get(0)).orElse(null);
        if (oldGroup != null) {
          oldGroup.clear();
          oldGroup.addAll(windowedGroup);
        } else {
          state.put(windowedGroup);
        }
      }
    });
    handle.saveGroupingState(state);
  }

  @Override
  public void onRecover(final GroupingState state, final AtomicHandle handle) {
    //noinspection unchecked
    this.state = state;
  }

  @Override
  public void onMinGTimeUpdate(final Meta meta) {
    final Consumer<List<DataItem<T>>> removeOldConsumer = group -> {
      int removeIndex = 0;
      //noinspection unchecked
      while (removeIndex < group.size() && group.get(group.size() - removeIndex - 1).meta().compareTo(meta) > 0) {
        removeIndex++;
      }
      group.subList(0, removeIndex).clear();
    };
    buffers.forEach(removeOldConsumer);
    state.forEach(removeOldConsumer);
  }
}
