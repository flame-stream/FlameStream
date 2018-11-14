package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.expleague.commons.util.Pair;
import org.jetbrains.annotations.Nullable;

import java.util.stream.Stream;

public interface StatefulOp<Input, State, Output> {
  Class<Input> inputClass();
  Class<Output> outputClass();
  State aggregate(Input in, @Nullable State state);

  default Output release(State state) {
    //noinspection unchecked
    return (Output)state;
  }

  default Stream<Pair<Output, State>> apply(Pair<Input, State> inputStatePair) {
    final State state = aggregate(inputStatePair.first, inputStatePair.second);
    return Stream.of(Pair.create(release(state), state));
  }
}
