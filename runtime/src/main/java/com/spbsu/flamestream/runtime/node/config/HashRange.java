package com.spbsu.flamestream.runtime.node.config;

import org.apache.commons.lang.math.IntRange;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

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
}
