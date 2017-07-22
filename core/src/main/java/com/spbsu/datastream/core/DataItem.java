package com.spbsu.datastream.core;

import com.spbsu.datastream.core.meta.Meta;

public interface DataItem<T> {
  Meta meta();

  T payload();

  long ack();
}
