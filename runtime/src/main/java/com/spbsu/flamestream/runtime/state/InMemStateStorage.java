package com.spbsu.flamestream.runtime.state;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.graph.state.GroupingState;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemStateStorage implements StateStorage {
  private final Map<HashUnit, Map<GlobalTime, Map<String, GroupingState>>> inner = new ConcurrentHashMap<>();

  @Override
  public Map<String, GroupingState> stateFor(HashUnit unit, GlobalTime time) {
    return inner.getOrDefault(unit, Collections.emptyMap()).getOrDefault(time, Collections.emptyMap());
  }

  @Override
  public void putState(HashUnit unit, GlobalTime time, Map<String, GroupingState> state) {
    inner.putIfAbsent(unit, new ConcurrentHashMap<>());
    inner.get(unit).put(time, state);
  }
}
