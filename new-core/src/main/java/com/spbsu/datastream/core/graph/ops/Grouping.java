package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.Processor;
import com.spbsu.datastream.core.graph.ops.grouping_storage.GroupingStorage;
import com.spbsu.datastream.core.graph.ops.grouping_storage.LazyGroupingStorage;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Grouping<T> extends Processor<T, List<T>> {
  private final Hash hash;
  private final int window;

  //logic
  private final GroupingStorage state;
  private final GroupingStorage buffers;

  public Grouping(final Hash hash, final int window) {
    this.hash = hash;
    this.window = window;
    state = new LazyGroupingStorage(hash);
    buffers = new LazyGroupingStorage(hash);
  }

  @Override
  public void onPush(final InPort inPort, final DataItem<?> item, final AtomicHandle handler) {
    List<DataItem> group = buffers.get(item).orElse(null);
    if (group != null) { // look for time collision in the current tick
      int replayCount = 0;
      //noinspection unchecked
      while (replayCount < group.size() && group.get(group.size() - replayCount - 1).meta().compareTo(item.meta()) > 0) {
        replayCount++;
      }
      group.add(group.size() - replayCount, item);
      if (replayCount > 0) {
        for (int i = group.size() - replayCount; i < group.size(); i++) {
          handler.push(outPort(), new DataItem(group.get(i).meta(), new ArrayList<>(group.subList(window > 0 ? Math.max(0, i + 1 - window) : 0, i + 1))));
        }
        return;
      }
    } else { // creating group from existing in the state
      group = new ArrayList<>(state.get(item).orElse(Collections.emptyList()));
      group.add(item);
      buffers.put(group);
    }
    handler.push(outPort(), new DataItem(item.meta(), window > 0 ? group.subList(Math.max(0, group.size() - window), group.size()) : group));
  }

  @Override
  public Graph deepCopy() {
    return new Grouping(hash, window);
  }
}
