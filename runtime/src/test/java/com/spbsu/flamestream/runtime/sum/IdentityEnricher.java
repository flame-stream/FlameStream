package com.spbsu.flamestream.runtime.sum;

import com.spbsu.flamestream.core.graph.SerializableFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

final class IdentityEnricher implements SerializableFunction<List<Numb>, Stream<List<Numb>>> {
  @Override
  public Stream<List<Numb>> apply(List<Numb> numberGroupingResult) {
    if (numberGroupingResult.size() == 1) {
      final List<Numb> group = new ArrayList<>();
      group.add(new Sum(0));
      group.add(numberGroupingResult.get(0));
      return Stream.of(group);
    } else {
      return Stream.of(numberGroupingResult);
    }
  }
}
