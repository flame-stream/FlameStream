package com.spbsu.flamestream.runtime.utils.collections;

import java.util.Set;

public interface IntRangeMap<V> {
  V get(int key);

  void putAll(IntRangeMap<V> map);

  Set<ListIntRangeMap.Entry<V>> entrySet();
}
