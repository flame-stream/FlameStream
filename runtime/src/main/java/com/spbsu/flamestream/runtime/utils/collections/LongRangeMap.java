package com.spbsu.flamestream.runtime.utils.collections;

import java.util.Collection;

public interface LongRangeMap<V> {
  V get(long key);

  Collection<V> values();
}
