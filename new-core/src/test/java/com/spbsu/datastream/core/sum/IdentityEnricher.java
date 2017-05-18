package com.spbsu.datastream.core.sum;

import com.spbsu.datastream.core.graph.ops.GroupingResult;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class IdentityEnricher implements Function<GroupingResult<Numb>, GroupingResult<Numb>> {
  @Override
  public GroupingResult<Numb> apply(GroupingResult<Numb> numberGroupingResult) {
    if (numberGroupingResult.payload().size() == 1) {
      final List<Numb> group = new ArrayList<>();
      group.add(new Sum(0));
      group.add(numberGroupingResult.payload().get(0));
      return new GroupingResult<>(group, numberGroupingResult.rootHash());
    } else {
      return numberGroupingResult;
    }
  }
}
