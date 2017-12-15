package com.spbsu.flamestream.runtime.sum;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

final class Reduce implements Function<List<Numb>, Stream<Sum>> {
  @Override
  public Stream<Sum> apply(List<Numb> group) {
    return Stream.of(new Sum(group.get(0).value() + group.get(1).value()));
  }
}
