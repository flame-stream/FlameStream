package com.spbsu.flamestream.core;

import java.util.stream.Stream;

public interface Batch {
  <T> Stream<T> payload(Class<T> clazz);

  enum Default implements Batch {
    EMPTY;

    @Override
    public <T> Stream<T> payload(Class<T> clazz) {
      return Stream.empty();
    }
  }
}
