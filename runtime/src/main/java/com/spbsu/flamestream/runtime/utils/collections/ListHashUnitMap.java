package com.spbsu.flamestream.runtime.utils.collections;

import com.spbsu.flamestream.runtime.config.HashUnit;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class ListHashUnitMap<T> implements HashUnitMap<T> {
  private final Set<Map.Entry<HashUnit, T>> mapping;

  public ListHashUnitMap() {
    mapping = new HashSet<>();
  }

  public ListHashUnitMap(Map<HashUnit, T> nodeMapping) {
    this.mapping = nodeMapping.entrySet()
            .stream()
            .map(e -> new Entry<>(e.getKey(), e.getValue()))
            .collect(toSet());
  }

  @Override
  public T get(int key) {
    for (final Map.Entry<HashUnit, T> entry : mapping) {
      if (entry.getKey().covers(key)) {
        return entry.getValue();
      }
    }

    return null;
  }

  @Override
  public void putAll(Map<HashUnit, T> map) {
    mapping.addAll(map.entrySet());
  }

  @Override
  public void put(HashUnit unit, T value) {
    mapping.add(new Entry<>(unit, value));
  }

  @Override
  public Set<Map.Entry<HashUnit, T>> entrySet() {
    return mapping;
  }

  public static final class Entry<V> implements Map.Entry<HashUnit, V> {
    private final HashUnit unit;
    private V value;

    public Entry(HashUnit unit, V value) {
      this.unit = unit;
      this.value = value;
    }

    @Override
    public HashUnit getKey() {
      return unit;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      final V tmp = value;
      this.value = value;
      return tmp;
    }
  }
}
