package com.spbsu.flamestream.core;

import java.util.stream.Stream;

public interface Batch {
  Batch EMPTY_BATCH = new Batch() {

    @Override
    public <T> Stream<T> payload(Class<T> clazz) {
      return Stream.empty();
    }
  };

  <T> Stream<T> payload(Class<T> clazz);
}
