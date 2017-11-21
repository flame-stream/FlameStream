package com.spbsu.flamestream.runtime.utils;

public interface HashMapping<V> {
  V valueFor(int hash);
}
