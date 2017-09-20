package com.spbsu.datastream.core.configuration;

import java.util.Objects;

public final class HashRange {
  private final int from;

  private final int to;

  public HashRange(int from, int to) {
    this.from = from;
    this.to = to;
  }

  public int from() {
    return from;
  }

  public int to() {
    return to;
  }

  public boolean contains(int hash) {
    return from <= hash && hash < to;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HashRange hashRange = (HashRange) o;
    return from == hashRange.from &&
            to == hashRange.to;
  }

  @Override
  public int hashCode() {
    return Objects.hash(from, to);
  }

  @Override
  public String toString() {
    return from + "_" + to;
  }
}
