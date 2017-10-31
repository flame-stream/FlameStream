package com.spbsu.flamestream.runtime.tick;

import com.spbsu.flamestream.runtime.range.HashRange;

import java.util.Map;

public interface HashMapping<T> {
  static <T> HashMapping<T> hashMapping(Map<HashRange, T> nodeMapping) {
    return new ListHashMapping<>(nodeMapping);
  }

  Map<HashRange, T> asMap();

  T valueFor(int hash);
}
