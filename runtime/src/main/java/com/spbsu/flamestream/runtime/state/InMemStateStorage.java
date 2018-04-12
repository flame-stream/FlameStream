package com.spbsu.flamestream.runtime.state;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.graph.state.GroupingState;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemStateStorage implements StateStorage {
  private final Map<HashUnit, NavigableMap<GlobalTime, Map<String, GroupingState>>> inner = new ConcurrentHashMap<>();

  @Override
  public Map<String, GroupingState> stateFor(HashUnit unit, GlobalTime time) {
    return inner.getOrDefault(unit, new ConcurrentSkipListMap<>()).getOrDefault(time, new ConcurrentSkipListMap<>());
  }

  @Override
  public void putState(HashUnit unit, GlobalTime time, Map<String, GroupingState> state) {
    inner.putIfAbsent(unit, new ConcurrentSkipListMap<>());
    final NavigableMap<GlobalTime, Map<String, GroupingState>> timeMap = inner.get(unit);
    if (timeMap.size() == 2) {
      timeMap.remove(timeMap.firstKey());
    }
    timeMap.put(time, state);
  }

  @Override
  public void close() throws Exception {

  }
}
