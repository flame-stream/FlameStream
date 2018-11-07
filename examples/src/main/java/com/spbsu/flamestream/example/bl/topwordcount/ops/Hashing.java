package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;

import java.util.function.BiPredicate;
import java.util.function.IntFunction;
import java.util.function.Predicate;

public interface Hashing<State> {
  default HashFunction hashFunction(HashFunction hashFunction) {
    return hashFunction;
  }

  default int hash(State state) {
    return 0;
  }

  default Equalz equalz(Equalz equalz) {
    return equalz;
  }

  default boolean equals(State left, State right) {
    return true;
  }
}
