package com.spbsu.flamestream.runtime.utils.collections;

import com.spbsu.flamestream.runtime.config.HashUnit;

import java.util.Map;
import java.util.Set;

public interface HashUnitMap<V> {
  V get(int key);

  void putAll(Map<HashUnit, V> map);

  void put(HashUnit unit, V value);

  Set<Map.Entry<HashUnit, V>> entrySet();
}
