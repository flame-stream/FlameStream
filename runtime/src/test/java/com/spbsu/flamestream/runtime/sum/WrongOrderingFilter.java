package com.spbsu.flamestream.runtime.sum;


import java.util.List;
import java.util.function.Predicate;

final class WrongOrderingFilter implements Predicate<List<Numb>> {

  @Override
  public boolean test(List<Numb> numberGroupingResult) {
    if (numberGroupingResult.size() != 2) {
      throw new IllegalStateException("Group size should equal 2");
    }

    return numberGroupingResult.get(0) instanceof Sum && numberGroupingResult.get(1) instanceof LongNumb;

  }
}
