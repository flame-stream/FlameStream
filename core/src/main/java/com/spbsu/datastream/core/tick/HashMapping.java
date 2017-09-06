package com.spbsu.datastream.core.tick;

import com.spbsu.datastream.core.configuration.HashRange;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.*;

public interface HashMapping {
  static HashMapping hashMapping(Map<HashRange, Integer> nodeMapping) {
    return new ListHashMapping(nodeMapping);
  }

  Map<HashRange, Integer> asMap();

  int workerForHash(int hash);
}

final class ListHashMapping implements HashMapping {
  private final List<RangeEntry> mapping;

  ListHashMapping(Map<HashRange, Integer> nodeMapping) {
    this.mapping = nodeMapping.entrySet().stream()
            .map(e -> new RangeEntry(e.getKey(), e.getValue()))
            .collect(toList());
  }

  @Override
  public Map<HashRange, Integer> asMap() {
    return mapping.stream().collect(toMap(e -> e.range, e -> e.node));
  }

  @Override
  public int workerForHash(int hash) {
    for (RangeEntry entry : mapping) {
      if (entry.range.contains(hash)) {
        return entry.node;
      }
    }

    throw new IllegalStateException("Hash ranges doesn't cover Integer space");
  }

  private static final class RangeEntry {
    public final HashRange range;
    public final int node;

    private RangeEntry(HashRange range, int node) {
      this.range = range;
      this.node = node;
    }
  }
}
