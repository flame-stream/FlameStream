package com.spbsu.datastream.core.tick;

import com.spbsu.datastream.core.configuration.HashRange;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public interface HashMapping<T> {
  static <T> HashMapping<T> hashMapping(Map<HashRange, T> nodeMapping) {
    return new ListHashMapping<T>(nodeMapping);
  }

  Map<HashRange, T> asMap();

  T valueFor(int hash);
}

final class ListHashMapping<T> implements HashMapping<T> {
  private final List<RangeEntry<T>> mapping;

  ListHashMapping(Map<HashRange, T> nodeMapping) {
    this.mapping = nodeMapping.entrySet().stream()
            .map(e -> new RangeEntry<>(e.getKey(), e.getValue()))
            .collect(toList());
  }

  @Override
  public Map<HashRange, T> asMap() {
    return mapping.stream().collect(toMap(e -> e.range, e -> e.node));
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
    public final HashRange range;
    public final T node;

    private RangeEntry(HashRange range, T node) {
      this.range = range;
      this.node = node;
    }
  }
}
