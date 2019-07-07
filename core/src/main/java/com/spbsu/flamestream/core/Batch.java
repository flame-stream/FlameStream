package com.spbsu.flamestream.core;

import java.util.stream.Stream;

public interface Batch {
  Stream<OutputPayload> payload();

  enum Default implements Batch {
    EMPTY;

    @Override
    public Stream<OutputPayload> payload() {
      return Stream.empty();
    }
  }
}
