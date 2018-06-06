package com.spbsu.flamestream.runtime.state;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.graph.state.GroupingState;

import java.util.Map;

public interface StateStorage extends AutoCloseable {
  /**
   * @return map from vertexId to its state
   */
  Map<String, GroupingState> stateFor(HashUnit unit, GlobalTime time);

  void putState(HashUnit unit, GlobalTime time, Map<String, GroupingState> state);
}
