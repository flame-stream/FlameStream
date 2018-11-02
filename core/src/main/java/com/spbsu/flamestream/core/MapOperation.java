package com.spbsu.flamestream.core;

import org.jetbrains.annotations.Nullable;

import java.util.function.Function;
import java.util.stream.Stream;

public interface MapOperation<Input, Output> extends Function<Input, Stream<Output>> {
  @Nullable
  default HashFunction hashing() {
    return null;
  }
}
