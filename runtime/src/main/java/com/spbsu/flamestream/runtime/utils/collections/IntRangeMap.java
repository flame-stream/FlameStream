package com.spbsu.flamestream.runtime.utils.collections;

import java.util.Collection;

public interface IntRangeMap<V> {
  V get(int key);

  Collection<V> values();
}
