package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.meta.Meta;

public interface DataItem<T> {
  Meta meta();

  T payload();

  long ack();
}
