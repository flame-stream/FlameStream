package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.stream.Stream;

public interface Batch {
  GlobalTime time();

  <T> Stream<T> payload(Class<T> clazz);

  enum Default implements Batch {
    EMPTY;

    @Override
    public GlobalTime time() {
      return GlobalTime.MIN;
    }

    @Override
    public <T> Stream<T> payload(Class<T> clazz) {
      return Stream.empty();
    }
  }
}
