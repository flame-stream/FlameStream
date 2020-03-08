package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.MinTimeUpdateListener;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class MinTimeUpdater {
  private final ArrayList<TrackingComponent> trackingComponents = new ArrayList<>();
  private final List<ActorRef> shards;
  private final long defaultMinimalTime;

  private static class TrackingComponent {
    private int index;
    private Map<ActorRef, ShardState> shardStates;
    private MinTimeUpdate lastMinTime;

    public TrackingComponent(int index, long defaultMinimalTime, List<ActorRef> shards) {
      this.index = index;
      lastMinTime = new MinTimeUpdate(index, new GlobalTime(defaultMinimalTime, EdgeId.MIN), new NodeTimes());
      shardStates = shards.stream().collect(Collectors.toMap(Function.identity(), __ -> new ShardState()));
    }

    @Nullable
    public MinTimeUpdate onShardMinTimeUpdate(ActorRef shard, MinTimeUpdate shardMinTimeUpdate) {
      final ShardState shardState = shardStates.get(shard);
      shardState.operationsTime = shardMinTimeUpdate.getNodeTimes();
      shardState.minTimeUpdates.put(shardMinTimeUpdate.minTime(), shardMinTimeUpdate.getNodeTimes());
      final MinTimeUpdate minAmongTables = minAmongTables();
      if (minAmongTables.minTime().compareTo(lastMinTime.minTime()) > 0) {
        this.lastMinTime = minAmongTables;
        return minAmongTables;
      }
      return null;
    }

    private MinTimeUpdate minAmongTables() {
      if (shardStates.isEmpty()) {
        return lastMinTime;
      }
      NodeTimes min =
              shardStates.values()
                      .stream()
                      .map(shardState -> shardState.operationsTime)
                      .reduce(new NodeTimes(), NodeTimes::min);
      for (ShardState shardState : shardStates.values()) {
        final Map.Entry<GlobalTime, NodeTimes> minTimeUpdate = shardState.minTimeUpdates.entrySet()
                .stream()
                .filter(entry -> entry.getValue().greaterThanOrNotComparableTo(min))
                .findFirst()
                .map(entry -> shardState.minTimeUpdates.lowerEntry(entry.getKey()))
                .orElse(shardState.minTimeUpdates.lastEntry());
        if (minTimeUpdate == null) {
          continue;
        }
        shardState.minTime = minTimeUpdate.getKey();
        shardState.minTimeUpdates.tailMap(shardState.minTime).clear();
      }
      return new MinTimeUpdate(
              index,
              Collections.min(shardStates.values()
                      .stream()
                      .map(shardState -> shardState.minTime)
                      .collect(Collectors.toList())),
              min
      );
    }

    private class ShardState {
      GlobalTime minTime = lastMinTime.minTime();
      NodeTimes operationsTime = new NodeTimes();
      TreeMap<GlobalTime, NodeTimes> minTimeUpdates = new TreeMap<GlobalTime, NodeTimes>();
    }
  }

  public MinTimeUpdater(List<ActorRef> shards, long defaultMinimalTime) {
    this.shards = shards;
    this.defaultMinimalTime = defaultMinimalTime;
  }

  public void subscribe(ActorRef self) {
    shards.forEach(acker -> acker.tell(new MinTimeUpdateListener(self), self));
  }

  @Nullable
  public MinTimeUpdate onShardMinTimeUpdate(ActorRef shard, MinTimeUpdate shardMinTimeUpdate) {
    final int trackingComponent = shardMinTimeUpdate.trackingComponent();
    for (int i = trackingComponents.size(); i < trackingComponent + 1; i++) {
      trackingComponents.add(new TrackingComponent(i, defaultMinimalTime, shards));
    }
    return trackingComponents.get(trackingComponent).onShardMinTimeUpdate(shard, shardMinTimeUpdate);
  }
}
