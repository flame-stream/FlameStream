package com.spbsu.flamestream.runtime.node.tick;

import com.spbsu.flamestream.runtime.node.tick.range.HashRange;

import java.util.Map;

public interface HashMapping<T> {
  static <T> HashMapping<T> hashMapping(Map<HashRange, T> nodeMapping) {
    return new ListHashMapping<>(nodeMapping);
  }

  T valueFor(int hash);
}
