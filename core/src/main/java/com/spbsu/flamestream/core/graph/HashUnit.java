package com.spbsu.flamestream.core.graph;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

public class HashUnit {
  public static int scale(int value, int min, int max) {
    return (int) (((long) value - min) * ((long) Integer.MAX_VALUE + 1 - Integer.MIN_VALUE) / ((long) max + 1 - min)
            + Integer.MIN_VALUE);
  }

  public static final HashUnit EMPTY = new HashUnit(Integer.MAX_VALUE, Integer.MIN_VALUE);

  private final String id;
  private final int from;
  private final int to;

  @JsonCreator
  public HashUnit(@JsonProperty("from") int from, @JsonProperty("to") int to) {
    this.from = from;
    this.to = to;
    id = from + "_" + to;
  }

  @JsonProperty("from")
  public int from() {
    return from;
  }

  @JsonProperty("to")
  public int to() {
    return to;
  }

  @JsonProperty("id")
  public String id() {
    return id;
  }

  @Override
  public String toString() {
    return "HashUnit{" +
            "from=" + from +
            ", to=" + to +
            '}';
  }

  public static Stream<HashUnit> covering(int n) {
    if (n <= 0) {
      throw new IllegalArgumentException(n + "");
    }
    final List<HashUnit> result = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      result.add(new HashUnit(scale(i, 0, n - 1), scale(i + 1, 0, n - 1) - 1));
    }
    return result.stream();
  }

  @JsonIgnore
  public boolean covers(int hash) {
    return from <= hash && hash <= to;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final HashUnit hashUnit = (HashUnit) o;
    return Objects.equals(id, hashUnit.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  public boolean isEmpty() { return to < from; }
}
