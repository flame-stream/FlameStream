package com.spbsu.flamestream.runtime.utils.collections;

import org.apache.commons.lang.math.IntRange;

import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class ListIntRangeMap<T> implements IntRangeMap<T> {
  private final Set<Entry<T>> mapping;

  public ListIntRangeMap() {
    mapping = new HashSet<>();
  }

  public ListIntRangeMap(Map<IntRange, T> nodeMapping) {
    this.mapping = nodeMapping.entrySet()
            .stream()
            .map(e -> new Entry<>(e.getKey(), e.getValue()))
            .collect(toSet());
  }

  @Override
  public T get(int key) {
    for (Entry<T> entry : mapping) {
      // FIXME: 24.12.2017 possible bug?
      if (entry.range.containsInteger(key)) {
        return entry.node;
      }
    }

    throw new NoSuchElementException("No value found for key " + key);
  }

  @Override
  public void putAll(IntRangeMap<T> map) {
    mapping.addAll(map.entrySet());
  }

  @Override
  public Set<Entry<T>> entrySet() {
    return mapping;
  }

  public static final class Entry<T> {
    final IntRange range;
    final T node;

    private Entry(IntRange range, T node) {
      this.range = range;
      this.node = node;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Entry<?> entry = (Entry<?>) o;
      return range.equals(entry.range);
    }

    @Override
    public int hashCode() {
      return range.hashCode();
    }
  }
}
