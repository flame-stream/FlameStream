package com.spbsu.datastream.core;

import com.spbsu.commons.func.types.SerializationRepository;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface DataItem<T> {
  Meta meta();

  T payload();

  interface Meta<T> extends Comparable<T> {
    int tick();
  }
}
