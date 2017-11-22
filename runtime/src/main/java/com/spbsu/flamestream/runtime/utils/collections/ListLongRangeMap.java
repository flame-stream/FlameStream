package com.spbsu.flamestream.runtime.utils.collections;

import org.apache.commons.lang.math.LongRange;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class ListLongRangeMap<V> implements LongRangeMap<V> {
  private final List<RangeEntry<V>> mapping;

  public ListLongRangeMap(Map<LongRange, V> nodeMapping) {
    this.mapping = nodeMapping.entrySet()
            .stream()
            .map(e -> new RangeEntry<>(e.getKey(), e.getValue()))
            .collect(toList());
  }

  @Override
  public V get(long key) {
    for (RangeEntry<V> entry : mapping) {
      if (entry.range.containsLong(key)) {
        return entry.node;
      }
    }

    throw new NoSuchElementException("No value found for key " + key);
  }

  @Override
  public Collection<V> values() {
    return mapping.stream().map(m -> m.node).collect(Collectors.toSet());
  }

  private static final class RangeEntry<T> {
    final LongRange range;
    final T node;

    private RangeEntry(LongRange range, T node) {
      this.range = range;
      this.node = node;
    }
  }
}
