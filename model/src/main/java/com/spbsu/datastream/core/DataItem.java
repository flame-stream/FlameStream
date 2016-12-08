package com.spbsu.datastream.core;

import org.jetbrains.annotations.Nullable;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface DataItem {
  Meta meta();

  CharSequence serializedData();

  @Nullable
  <T> T as(Class<T> type);

  interface Meta<T extends Meta> extends Comparable<T> {
    int tick();

    T advanced();
  }

  interface Grouping {
    long hash(DataItem item);

    boolean equals(DataItem left, DataItem right);
  }
}
