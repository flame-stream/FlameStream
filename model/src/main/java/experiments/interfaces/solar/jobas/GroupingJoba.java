package experiments.interfaces.solar.jobas;

import experiments.interfaces.solar.*;
import experiments.interfaces.solar.items.ListDataItem;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class GroupingJoba extends Joba.Stub {
  private final TLongObjectHashMap<List<List<DataItem>>> state;
  private final TLongObjectHashMap<List<List<DataItem>>> buffers = new TLongObjectHashMap<>();
  private final Joba base;
  private final DataItem.Grouping grouping;
  private final int window;

  public GroupingJoba(Joba base, DataType generates, DataItem.Grouping grouping, int window) {
    super(generates);
    this.base = base;
    this.grouping = grouping;
    this.window = window;
    state = Output.instance().load(generates);
    Output.instance().registerCommitHandler(() -> {
      updateStates();
      Output.instance().save(generates, state);
    });
  }

  private void updateStates() {
    buffers.forEachEntry((hash, bucket) -> {
      bucket.forEach(group -> {
        final List<DataItem> windowedGroup = group.subList(window > 0 ? Math.max(0, group.size() - window) : 0, group.size());
        final List<DataItem> oldGroup = searchBucket(hash, group.get(0), state).orElse(null);
        if (oldGroup != null) {
          oldGroup.clear();
          oldGroup.addAll(windowedGroup);
        }
        else {
          state.putIfAbsent(hash, new ArrayList<>());
          state.get(hash).add(windowedGroup);
        }
      });
      return false;
    });
    buffers.clear();
  }

  private Optional<List<DataItem>> searchBucket(long hash, DataItem item, TLongObjectHashMap<List<List<DataItem>>> through) {
    return Stream.of(through.get(hash))
        .flatMap(state -> state != null ? state.stream() : Stream.empty())
        .filter(bucket -> bucket.isEmpty() || grouping.equals(bucket.get(0), item)).findAny();
  }

  @Override
  public Stream<DataItem> materialize(Stream<DataItem> seed) {
    final Spliterator<DataItem> spliterator = base.materialize(seed).spliterator();
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<DataItem>() {
      ListDataItem next = null;
      List<ListDataItem> replay;
      int replayIndex;

      @Override
      public boolean hasNext() {
        if (replay != null) {
          if (replayIndex < replay.size()) {
            next = replay.get(replayIndex++);
            return true;
          }
          else replay = null;
        }
        return spliterator.tryAdvance(item -> {
          final long hash = grouping.hash(item);
          List<DataItem> group = searchBucket(hash, item, buffers).orElse(null);
          if (group != null) { // look for time collision in the current tick
            int replayCount = 0;
            while (replayCount < group.size() && group.get(group.size() - replayCount - 1).meta().time().greater(item.meta().time())) {
              replayCount++;
            }
            group.add(group.size() - replayCount, item);
            if (replayCount > 0) {
              replay = new ArrayList<>(replayCount);
              for (int i = group.size() - replayCount; i < group.size(); i++) {
                replay.add(new ListDataItem(group.subList(window > 0 ? Math.max(0, i + 1 - window) : 0, i + 1), group.get(i).meta(), id()));
              }
              next = replay.get(0);
              replayIndex = 1;
              return;
            }
          }
          else { // creating group from existing in the state
            group = new ArrayList<>(searchBucket(hash, item, state).orElse(Collections.emptyList()));
            buffers.putIfAbsent(hash, new ArrayList<>());
            final List<List<DataItem>> lists = buffers.get(hash);
            lists.add(group);
            group.add(item);
          }
          next = new ListDataItem(window > 0 ? group.subList(Math.max(0, group.size() - window), group.size()) : group, item.meta(), id());
        });
      }

      @Override
      public DataItem next() {
        return next;
      }
    }, Spliterator.IMMUTABLE), false);
  }
}
