package com.spbsu.flamestream.runtime.sum;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class IdentityEnricher implements Function<List<Numb>, List<Numb>> {
  @Override
  public List<Numb> apply(List<Numb> numberGroupingResult) {
    if (numberGroupingResult.size() == 1) {
      final List<Numb> group = new ArrayList<>();
      group.add(new Sum(0));
      group.add(numberGroupingResult.get(0));
      return group;
    } else {
      return numberGroupingResult;
    }
  }
}
