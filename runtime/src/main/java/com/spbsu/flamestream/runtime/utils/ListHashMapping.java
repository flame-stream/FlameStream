package com.spbsu.flamestream.runtime.utils;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class ListHashMapping<T> implements HashMapping<T> {
  private final List<RangeEntry<T>> mapping;

  public ListHashMapping(Map<HashRange, T> nodeMapping) {
    this.mapping = nodeMapping.entrySet()
            .stream()
            .map(e -> new RangeEntry<>(e.getKey(), e.getValue()))
            .collect(toList());
  }

  @Override
  public T valueFor(int hash) {
    for (RangeEntry<T> entry : mapping) {
      if (entry.range.contains(hash)) {
        return entry.node;
      }
    }

    throw new IllegalStateException("Hash ranges doesn't cover Integer space");
  }

  private static final class RangeEntry<T> {
    final HashRange range;
    final T node;

    private RangeEntry(HashRange range, T node) {
      this.range = range;
      this.node = node;
    }
  }
}
