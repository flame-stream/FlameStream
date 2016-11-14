package com.spbsu.datastream.core.job;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.UntypedActor;
import com.spbsu.akka.ActorAdapter;
import com.spbsu.akka.ActorContainer;
import com.spbsu.akka.ActorMethod;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.io.Output;
import com.spbsu.datastream.core.item.ListDataItem;
import com.spbsu.datastream.core.job.control.Control;
import com.spbsu.datastream.core.job.control.EndOfTick;
import com.spbsu.datastream.core.state.GroupingState;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class NewGroupingJoba extends Joba.Stub {
  private final DataItem.Grouping grouping;
  private final int window;
  private final GroupingState state;

  public NewGroupingJoba(Joba base, DataType generates, DataItem.Grouping grouping, int window) {
    super(generates, base);
    this.grouping = grouping;
    this.window = window;
    state = DataStreamsContext.stateRepository.load(generates);
  }

  private Optional<List<DataItem>> searchBucket(long hash, DataItem item, TLongObjectHashMap<List<List<DataItem>>> through) {
    return Stream.of(through.get(hash))
            .flatMap(state -> state != null ? state.stream() : Stream.empty())
            .filter(bucket -> bucket.isEmpty() || grouping.equals(bucket.get(0), item)).findAny();
  }

  @Override
  protected ActorRef actor(ActorSystem at, ActorRef sink) {
    return at.actorOf(ActorContainer.props(GroupingActor.class, this, state, sink));
  }

  @SuppressWarnings({"WeakerAccess", "unused"})
  public static class GroupingActor extends ActorAdapter<UntypedActor> {
    private final TLongObjectHashMap<List<List<DataItem>>> state;
    private final TLongObjectHashMap<List<List<DataItem>>> buffers = new TLongObjectHashMap<>();
    private final ActorRef sink;
    private final NewGroupingJoba padre;
    boolean eos = false;

    public GroupingActor(NewGroupingJoba padre, TLongObjectHashMap<List<List<DataItem>>> state, ActorRef sink) {
      this.padre = padre;
      this.state = state;
      this.sink = sink;
    }

    @ActorMethod
    public void group(DataItem item) {
      final long hash = padre.grouping.hash(item);
      List<DataItem> group = padre.searchBucket(hash, item, buffers).orElse(null);
      final int window = padre.window;
      final int jobaId = padre.id();
      if (group != null) { // look for time collision in the current tick
        int replayCount = 0;
        while (replayCount < group.size() && group.get(group.size() - replayCount - 1).meta().compareTo(item.meta()) > 0) {
          replayCount++;
        }
        group.add(group.size() - replayCount, item);
        if (replayCount > 0) {
          for (int i = group.size() - replayCount; i < group.size(); i++) {
            sink.tell(new ListDataItem(group.subList(window > 0 ? Math.max(0, i + 1 - window) : 0, i + 1), group.get(i).meta()), self());
          }
          return;
        }
      } else { // creating group from existing in the state
        group = new ArrayList<>(padre.searchBucket(hash, item, state).orElse(Collections.emptyList()));
        buffers.putIfAbsent(hash, new ArrayList<>());
        final List<List<DataItem>> lists = buffers.get(hash);
        lists.add(group);
        group.add(item);
      }
      sink.tell(new ListDataItem(window > 0 ? group.subList(Math.max(0, group.size() - window), group.size()) : group, item.meta()), self());
    }

    @ActorMethod
    public void control(Control eot) {
      sink.tell(eot, sender());
      if (eot instanceof EndOfTick) {
        synchronized (state) {
          buffers.forEachEntry((hash, bucket) -> {
            bucket.forEach(group -> {
              final int window = padre.window;
              final List<DataItem> windowedGroup = group.subList(window > 0 ? Math.max(0, group.size() - window) : 0, group.size());
              final List<DataItem> oldGroup = padre.searchBucket(hash, group.get(0), state).orElse(null);
              if (oldGroup != null) {
                oldGroup.clear();
                oldGroup.addAll(windowedGroup);
              } else {
                state.putIfAbsent(hash, new ArrayList<>());
                state.get(hash).add(windowedGroup);
              }
            });
            return true;
          });
          Output.instance().save(padre.generates(), state);
        }

        context().stop(self());
      }
    }
  }
}
