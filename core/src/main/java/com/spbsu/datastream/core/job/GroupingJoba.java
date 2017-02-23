package com.spbsu.datastream.core.job;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.dataitem.ListDataItem;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.core.job.grouping_storage.GroupingStorage;
import com.spbsu.datastream.core.job.grouping_storage.LazyGroupingStorage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class GroupingJoba extends Joba.AbstractJoba {
  private final Sink sink;
  private final DataItem.Grouping grouping;
  private final int window;
  private final GroupingStorage state;
  private final GroupingStorage buffers;

  public GroupingJoba(Sink sink, DataType generates, DataItem.Grouping grouping, int window) {
    super(generates);
    this.sink = sink;
    this.grouping = grouping;
    this.window = window;

    buffers = new LazyGroupingStorage(grouping);
    state = DataStreamsContext.output.load(generates).orElse(new LazyGroupingStorage(grouping));
  }

  public void accept(DataItem item) {
    final long hash = grouping.hash(item);
    List<DataItem> group = buffers.get(hash, item).orElse(null);
    if (group != null) { // look for time collision in the current tick
      int replayCount = 0;
      //noinspection unchecked
      while (replayCount < group.size() && group.get(group.size() - replayCount - 1).meta().compareTo(item.meta()) > 0) {
        replayCount++;
      }
      group.add(group.size() - replayCount, item);
      if (replayCount > 0) {
        for (int i = group.size() - replayCount; i < group.size(); i++) {
          sink.accept(new ListDataItem(group.subList(window > 0 ? Math.max(0, i + 1 - window) : 0, i + 1), group.get(i).meta()));
        }
        return;
      }
    } else { // creating group from existing in the state
      group = new ArrayList<>(state.get(hash, item).orElse(Collections.emptyList()));
      group.add(item);
      buffers.put(group);
    }
    sink.accept(new ListDataItem(window > 0 ? group.subList(Math.max(0, group.size() - window), group.size()) : group, item.meta()));
  }

  public void accept(Control eot) {
    if (eot instanceof EndOfTick) {
      synchronized (state) {
        buffers.forEach((hash, group) -> {
          final List<DataItem> windowedGroup = group.subList(window > 0 ? Math.max(0, group.size() - window) : 0, group.size());
          if (!windowedGroup.isEmpty()) {
            final List<DataItem> oldGroup = state.get(hash, group.get(0)).orElse(null);
            if (oldGroup != null) {
              oldGroup.clear();
              oldGroup.addAll(windowedGroup);
            } else {
              state.put(windowedGroup);
            }
          }
          return true;
        });
        DataStreamsContext.output.save(generates(), state);
      }
    }
    sink.accept(eot);
  }
}
