package com.spbsu.datastream.core;

import java.util.Objects;

public final class HashRange {
  private final int from;

  private final int to;

  public HashRange(final int from, final int to) {
    this.from = from;
    this.to = to;
  }

  public static HashRange fromString(final String serializedHashRange) {
    final String[] arr = serializedHashRange.split("_");
    return new HashRange(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]));
  }

  public boolean isIn(final int hash) {
    return this.from <= hash && hash < this.to;
  }

  public int from() {
    return this.from;
  }

  public int to() {
    return this.to;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    final HashRange hashRange = (HashRange) o;
    return this.from == hashRange.from &&
            this.to == hashRange.to;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.from, this.to);
  }

  @Override
  public String toString() {
    return this.from + "_" + this.to;
  }
}
