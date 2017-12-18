package com.spbsu.flamestream.runtime.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class ComputationProps {
  private final int maxElementsInGraph;
  private final Map<String, HashRange> ranges;

  @JsonCreator
  public ComputationProps(@JsonProperty("ranges") Map<String, HashRange> ranges,
                          @JsonProperty("max-graph-elements") int maxElementsInGraph) {
    this.ranges = ranges;
    this.maxElementsInGraph = maxElementsInGraph;
  }

  @JsonProperty("ranges")
  public Map<String, HashRange> ranges() {
    return ranges;
  }

  @JsonProperty("max-graph-elements")
  public int maxElementsInGraph() {
    return this.maxElementsInGraph;
  }

  @Override
  public String toString() {
    return "ComputationLayout{" +
            "ranges=" + ranges +
            '}';
  }
}
