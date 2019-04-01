package com.spbsu.flamestream.runtime.master.acker;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class MinTimeUpdater<Shard> {
  private class ShardState {
    GlobalTime minTime = lastMinTime.minTime();
    JobaTimes operationsTime = new JobaTimes();
    TreeMap<GlobalTime, JobaTimes> minTimeUpdates = new TreeMap<>();
  }

  private Map<Shard, ShardState> shardStates;

  private MinTimeUpdate lastMinTime = new MinTimeUpdate(GlobalTime.MIN, new JobaTimes());

  public MinTimeUpdater(List<Shard> shards) {
    this.shardStates = shards.stream().collect(Collectors.toMap(Function.identity(), __ -> new ShardState()));
  }

  @Nullable
  public MinTimeUpdate onShardMinTimeUpdate(Shard shard, MinTimeUpdate shardMinTimeUpdate) {
    ShardState shardState = shardStates.get(shard);
    shardState.operationsTime = shardMinTimeUpdate.getJobaTimes();
    if (shardState.minTimeUpdates.containsKey(shardMinTimeUpdate.minTime())) {
      throw new RuntimeException("should not be there");
    }
    shardState.minTimeUpdates.put(shardMinTimeUpdate.minTime(), shardMinTimeUpdate.getJobaTimes());
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
    JobaTimes min =
            shardStates.values()
                    .stream()
                    .map(shardState -> shardState.operationsTime)
                    .reduce(new JobaTimes(), JobaTimes::min);
    for (ShardState shardState : shardStates.values()) {
      final Map.Entry<GlobalTime, JobaTimes> minTimeUpdate = shardState.minTimeUpdates.entrySet()
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
    return new MinTimeUpdate(Collections.min(shardStates.values()
            .stream()
            .map(shardState -> shardState.minTime)
            .collect(Collectors.toList())), min);
  }
}
