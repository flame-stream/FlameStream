package com.spbsu.flamestream.runtime.config;

import org.apache.commons.lang.math.IntRange;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class HashRange {
  private final int from;
  private final int to;

  @JsonCreator
  public HashRange(@JsonProperty("from") int from, @JsonProperty("to") int to) {
    this.from = from;
    this.to = to;
  }

  @JsonProperty("from")
  public int from() {
    return from;
  }

  @JsonProperty("to")
  public int to() {
    return to;
  }

  @JsonIgnore
  public IntRange asRange() {
    return new IntRange(from, to);
  }

  @Override
  public String toString() {
    return "HashRange{" +
            "from=" + from +
            ", to=" + to +
            '}';
  }

  public static Stream<HashRange> covering(int n) {
    final Set<HashRange> result = new HashSet<>();
    final int step = (int) (((long) Integer.MAX_VALUE - Integer.MIN_VALUE) / n);
    long left = Integer.MIN_VALUE;
    long right = left + step;
    for (int i = 0; i < n; ++i) {
      result.add(new HashRange((int) left, (int) right));
      left += step;
      right = Math.min(Integer.MAX_VALUE, right + step);
    }
    return result.stream();
  }
}
