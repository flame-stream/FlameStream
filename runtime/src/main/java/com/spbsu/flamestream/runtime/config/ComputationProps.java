package com.spbsu.flamestream.runtime.config;

import com.spbsu.flamestream.core.graph.HashGroup;

import java.util.Map;

public class ComputationProps {
  private final int maxElementsInGraph;
  private final boolean barrierIsDisabled;
  private final long defaultMinimalTime;
  private final Map<String, HashGroup> hashGroups;

  public ComputationProps(
          Map<String, HashGroup> hashGroups,
          int maxElementsInGraph,
          boolean barrierIsDisabled,
          long defaultMinimalTime
  ) {
    this.hashGroups = hashGroups;
    this.maxElementsInGraph = maxElementsInGraph;
    this.barrierIsDisabled = barrierIsDisabled;
    this.defaultMinimalTime = defaultMinimalTime;
  }

  public Map<String, HashGroup> hashGroups() {
    return hashGroups;
  }

  public int maxElementsInGraph() {
    return this.maxElementsInGraph;
  }

  @Override
  public String toString() {
    return "ComputationLayout{" +
            "hashGroups=" + hashGroups +
            '}';
  }

  public boolean barrierIsDisabled() {
    return barrierIsDisabled;
  }

  public long defaultMinimalTime() {
    return defaultMinimalTime;
  }
}
