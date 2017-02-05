package com.spbsu.datastream.core;

import org.jetbrains.annotations.Nullable;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface DataItem<T> {
  Meta meta();

  CharSequence serializedData();

  @Nullable
  T as(Class<T> type);

  interface Meta<T extends Meta> extends Comparable<T> {
    int tick();

    /***
     * @return new instance of Meta = this + little bit
     */
    T advanced();
  }

}
