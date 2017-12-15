package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.data.meta.Meta;

public interface DataItem {
  Meta meta();

  <T> T payload(Class<T> expectedClass);

  long xor();
}
