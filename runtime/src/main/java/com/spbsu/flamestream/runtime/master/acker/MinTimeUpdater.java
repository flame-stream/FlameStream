package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class MinTimeUpdater {
  private class ShardState {
    GlobalTime minTime = lastMinTime;
    JobaTimes operationsTime = new JobaTimes();
    TreeMap<GlobalTime, JobaTimes> minTimeUpdates = new TreeMap<>();
  }

  private Map<ActorRef, ShardState> shardStates;

  private GlobalTime lastMinTime = GlobalTime.MIN;

  public MinTimeUpdater(List<ActorRef> shards) {
    this.shardStates = shards.stream().collect(Collectors.toMap(Function.identity(), __ -> new ShardState()));
  }

  @Nullable
  public GlobalTime onShardMinTimeUpdate(ActorRef shard, MinTimeUpdate shardMinTimeUpdate) {
    ShardState shardState = shardStates.get(shard);
    shardState.operationsTime = shardMinTimeUpdate.getJobaTimes();
    if (shardState.minTimeUpdates.containsKey(shardMinTimeUpdate.minTime())) {
      throw new RuntimeException("should not be there");
    }
    shardState.minTimeUpdates.put(shardMinTimeUpdate.minTime(), shardMinTimeUpdate.getJobaTimes());
    final GlobalTime minAmongTables = minAmongTables();
    if (minAmongTables.compareTo(lastMinTime) > 0) {
      this.lastMinTime = minAmongTables;
      return minAmongTables;
    }
    return null;
  }

  private GlobalTime minAmongTables() {
    JobaTimes min =
            shardStates.values()
                    .stream()
                    .map(shardState -> shardState.operationsTime)
                    .reduce(new JobaTimes(), JobaTimes::min);
    for (ShardState shardState : shardStates.values()) {
      shardState.minTimeUpdates.entrySet()
              .stream()
              .filter(entry -> entry.getValue().greaterThanOrNotComparableTo(min))
              .findFirst().ifPresent(entry -> {
        shardState.minTime = entry.getKey();
        shardState.minTimeUpdates.tailMap(entry.getKey()).clear();
      });
    }
    if (shardStates.isEmpty()) {
      return lastMinTime;
    }
    return Collections.min(shardStates.values()
            .stream()
            .map(shardState -> shardState.minTime)
            .collect(Collectors.toList()));
  }
}
