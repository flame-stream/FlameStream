package com.spbsu.datastream.core;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public final class DataItem<T> implements Comparable<DataItem> {
  private final Meta meta;

  private final T payload;

  public DataItem(final Meta meta, final T payload) {
    this.meta = meta;
    this.payload = payload;
  }

  public Meta meta() {
    return meta;
  }

  public T payload() {
    return payload;
  }

  public static <T> DataItem<T> propagate(final DataItem<T> old, final int stageHash) {
    return new DataItem<>(new Meta(old.meta(), stageHash), old.payload());
  }

  @Override
  public int compareTo(@NotNull final DataItem o) {
    return this.meta.compareTo(o.meta);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final DataItem<?> dataItem = (DataItem<?>) o;
    return Objects.equals(meta, dataItem.meta) &&
            Objects.equals(payload, dataItem.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(meta, payload);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DataItem{");
    sb.append("meta=").append(meta);
    sb.append(", payload=").append(payload);
    sb.append('}');
    return sb.toString();
  }
}
