package com.spbsu.datastream.core.sum;

import com.spbsu.datastream.core.graph.ops.GroupingResult;

import java.util.List;
import java.util.function.Function;

public final class Reduce implements Function<GroupingResult<Numb>, Sum> {
  @Override
  public Sum apply(GroupingResult<Numb> numberGroupingResult) {
    final List<Numb> group = numberGroupingResult.payload();
    return new Sum(group.get(0).value() + group.get(1).value());
  }
}
