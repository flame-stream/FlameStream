package com.spbsu.flamestream.runtime.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class ComputationProps {
  private final int maxElementsInGraph;
  private final Map<String, HashGroup> hashGroups;

  @JsonCreator
  public ComputationProps(@JsonProperty("hashGroups") Map<String, HashGroup> hashGroups,
                          @JsonProperty("maxElementsInGraph") int maxElementsInGraph) {
    this.hashGroups = hashGroups;
    this.maxElementsInGraph = maxElementsInGraph;
  }

  @JsonProperty
  public Map<String, HashGroup> hashGroups() {
    return hashGroups;
  }

  @JsonProperty
  public int maxElementsInGraph() {
    return this.maxElementsInGraph;
  }

  @Override
  public String toString() {
    return "ComputationLayout{" +
            "hashGroups=" + hashGroups +
            '}';
  }
}
