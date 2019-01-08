package com.spbsu.flamestream.runtime.graph.state;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.utils.collections.HashUnitMap;
import com.spbsu.flamestream.runtime.utils.collections.ListHashUnitMap;

import java.util.Collection;

public class GroupGroupingState {
  private final HashUnitMap<GroupingState> unitStates = new ListHashUnitMap<>();

  public GroupGroupingState() {
  }

  public GroupGroupingState(Collection<HashUnit> units) {
    units.forEach(u -> unitStates.put(u, new GroupingState()));
  }

  public void addUnitState(HashUnit unit, GroupingState unitState) {
    unitStates.put(unit, unitState);
  }

  public InvalidatingBucket bucketFor(DataItem item, HashFunction hashFunction, Equalz equalz) {
    final int hash = hashFunction.hash(item);
    final GroupingState state = unitStates.get(hash);
    if (state == null) {
      System.out.format("SSS-1: %s %d %s%n", state, hash, item);
      System.out.format("SSS-2: %s%n", unitStates);
    }
    InvalidatingBucket res = state.bucketFor(item, hashFunction, equalz);
    //System.out.println("RES: " + res.size());
    if (res.size() > 0) {
    //  System.out.format("11111: %s %s%n", item.payload(Object.class), res.get(0).payload(Object.class));
    }
    return res;
  }
}
