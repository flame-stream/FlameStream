package com.spbsu.datastream.core.graph.ops;

import java.util.Objects;

public class HashRange {
  private final int from;

  private final int to;

  public HashRange(final int from, final int to) {
    this.from = from;
    this.to = to;
  }

  public boolean isIn(int hash) {
    return from <= hash && hash < to;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
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
    return "HashRange{" + "from=" + from +
            ", to=" + to +
            '}';
  }
}
