package com.spbsu.flamestream.runtime.state;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.graph.state.GroupingState;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class DevNullStateStorage implements StateStorage {
  @Override
  public Map<String, GroupingState> stateFor(HashUnit unit, GlobalTime time) {
    return new ConcurrentSkipListMap<>();
  }

  @Override
  public void putState(HashUnit unit, GlobalTime time, Map<String, GroupingState> state) {
  }

  @Override
  public void close() {
  }
}
