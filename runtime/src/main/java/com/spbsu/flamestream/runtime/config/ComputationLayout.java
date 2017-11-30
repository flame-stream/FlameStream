package com.spbsu.flamestream.runtime.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class ComputationLayout {
  private final Map<String, HashRange> ranges;

  @JsonCreator
  public ComputationLayout(@JsonProperty("ranges") Map<String, HashRange> ranges) {
    this.ranges = ranges;
  }

  @JsonProperty("ranges")
  public Map<String, HashRange> ranges() {
    return ranges;
  }

  @Override
  public String toString() {
    return "ComputationLayout{" +
            "ranges=" + ranges +
            '}';
  }
}
