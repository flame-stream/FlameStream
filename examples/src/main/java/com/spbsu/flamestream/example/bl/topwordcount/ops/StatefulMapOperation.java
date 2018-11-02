package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.expleague.commons.util.Pair;
import com.spbsu.flamestream.core.MapOperation;
import org.jetbrains.annotations.Nullable;

import java.util.stream.Stream;

public interface StatefulMapOperation<Input, State, Output> extends MapOperation<Pair<Input, State>, Pair<Output, State>> {
  State aggregate(Input in, @Nullable State state);

  default Output release(State state) {
    //noinspection unchecked
    return (Output)state;
  }

  @Override
  default Stream<Pair<Output, State>> apply(Pair<Input, State> inputStatePair) {
    final State state = aggregate(inputStatePair.first, inputStatePair.second);
    return Stream.of(Pair.create(release(state), state));
  }
}
