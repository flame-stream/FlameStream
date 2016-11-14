package com.spbsu.datastream.core.state;

import com.spbsu.datastream.core.DataType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by marnikitta on 14.11.16.
 */
public class StateRepository {
  private final Map<String, GroupingState> states = new HashMap<>();


  public GroupingState load(DataType type) {
    return states.getOrDefault(type.name(), new GroupingState());
  }
}
