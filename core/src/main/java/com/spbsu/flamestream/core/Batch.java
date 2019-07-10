package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.stream.Stream;

public interface Batch {
  GlobalTime time();

  Stream<DataItem> payload();

  enum Default implements Batch {
    EMPTY;

    @Override
    public GlobalTime time() {
      return GlobalTime.MIN;
    }

    @Override
    public Stream<DataItem> payload() {
      return Stream.empty();
    }
  }
}
