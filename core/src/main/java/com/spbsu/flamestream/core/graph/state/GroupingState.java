package com.spbsu.flamestream.core.graph.state;

import gnu.trove.map.TIntObjectMap;

/**
 * User: Artem
 * Date: 21.11.2017
 */
public interface GroupingState {
  TIntObjectMap<Object> state();
}
