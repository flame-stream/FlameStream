package com.spbsu.flamestream.runtime.config;

public class AckerConfig {
  private final int maxElementsInGraph;
  private final int millisBetweenCommits;
  private final long defaultMinimalTime;

  public AckerConfig(int maxElementsInGraph, int millisBetweenCommits, long defaultMinimalTime) {
    this.maxElementsInGraph = maxElementsInGraph;
    this.millisBetweenCommits = millisBetweenCommits;
    this.defaultMinimalTime = defaultMinimalTime;
  }

  public long defaultMinimalTime() {
    return defaultMinimalTime;
  }

  public int maxElementsInGraph() {
    return maxElementsInGraph;
  }

  public int millisBetweenCommits() {
    return this.millisBetweenCommits;
  }

  @Override
  public String toString() {
    return "AckerConfig{maxElementsInGraph=" + maxElementsInGraph + '}';
  }
}
