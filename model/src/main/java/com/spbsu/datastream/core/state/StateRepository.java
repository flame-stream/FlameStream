package com.spbsu.datastream.core.state;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Created by marnikitta on 14.11.16.
 */
public class StateRepository {
  private final Map<String,GroupingState> states = new HashMap<>();

  public GroupingState load(String type) {
    return Optional.ofNullable(states.get(type)).orElse(new GroupingState());
  }

  public synchronized void update(String type, Function<Optional<GroupingState>, GroupingState> updater) {
    states.put(type, updater.apply(Optional.ofNullable(states.get(type))));
  }
}
