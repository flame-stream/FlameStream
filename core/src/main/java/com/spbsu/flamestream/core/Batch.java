package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public interface Batch {
  GlobalTime time();

  Stream<DataItem> payload();

  Map<Long, Instant> lastGlobalTimeProcessedAt();

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

    @Override
    public Map<Long, Instant> lastGlobalTimeProcessedAt() {
      return Collections.emptyMap();
    }
  }
}
