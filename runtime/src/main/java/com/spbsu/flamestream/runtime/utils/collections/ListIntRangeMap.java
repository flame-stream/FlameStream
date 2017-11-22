package com.spbsu.flamestream.runtime.utils.collections;

import org.apache.commons.lang.math.IntRange;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class ListIntRangeMap<T> implements IntRangeMap<T> {
  private final List<RangeEntry<T>> mapping;

  public ListIntRangeMap(Map<IntRange, T> nodeMapping) {
    this.mapping = nodeMapping.entrySet()
            .stream()
            .map(e -> new RangeEntry<>(e.getKey(), e.getValue()))
            .collect(toList());
  }

  @Override
  public T get(int key) {
    for (RangeEntry<T> entry : mapping) {
      if (entry.range.containsInteger(key)) {
        return entry.node;
      }
    }

    throw new NoSuchElementException("No value found for key " + key);
  }

  @Override
  public Collection<T> values() {
    return mapping.stream().map(m -> m.node).collect(Collectors.toSet());
  }

  private static final class RangeEntry<T> {
    final IntRange range;
    final T node;

    private RangeEntry(IntRange range, T node) {
      this.range = range;
      this.node = node;
    }
  }
}
