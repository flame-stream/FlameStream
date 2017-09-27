package com.spbsu.flamestream.runtime.sum;

import java.util.List;
import java.util.function.Function;

public final class Reduce implements Function<List<Numb>, Sum> {
  @Override
  public Sum apply(List<Numb> group) {
    return new Sum(group.get(0).value() + group.get(1).value());
  }
}
