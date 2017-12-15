package com.spbsu.flamestream.runtime.sum;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

final class WrongOrderingFilter implements Function<List<Numb>, Stream<List<Numb>>> {

  @Override
  public Stream<List<Numb>> apply(List<Numb> numbs) {
    if (numbs.size() != 2) {
      throw new IllegalStateException("Group size should equal 2");
    }

    if (numbs.get(0) instanceof Sum && numbs.get(1) instanceof LongNumb) {
      return Stream.of(numbs);
    } else {
      return Stream.empty();
    }
  }
}
