package com.spbsu.datastream.core.sum;

import com.spbsu.datastream.core.graph.ops.GroupingResult;

import java.util.List;
import java.util.function.Predicate;

public final class WrongOrderingFilter implements Predicate<GroupingResult<Numb>> {

  @Override
  public boolean test(GroupingResult<Numb> numberGroupingResult) {
    final List<Numb> payload = numberGroupingResult.payload();
    if (payload.size() != 2) {
      throw new IllegalStateException("Group size should equal 2");
    } else if (payload.get(0) instanceof Sum && payload.get(1) instanceof LongNumb) {
      return true;
    }

    return false;
  }
}
