package com.spbsu.flamestream.core.graph.state;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * User: Artem
 * Date: 21.11.2017
 */
public class GroupingStateImpl implements GroupingState {
  private final TIntObjectMap<Object> state = new TIntObjectHashMap<>();

  @Override
  public TIntObjectMap<Object> state() {
    return state;
  }
}
