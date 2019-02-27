package com.spbsu.flamestream.runtime.config;

public class CommitterConfig {
  private final int maxElementsInGraph;
  private final int millisBetweenCommits;
  private final long defaultMinimalTime;
  private final boolean distributedAcker;

  public CommitterConfig(
          int maxElementsInGraph,
          int millisBetweenCommits,
          long defaultMinimalTime,
          boolean distributedAcker
  ) {
    this.maxElementsInGraph = maxElementsInGraph;
    this.millisBetweenCommits = millisBetweenCommits;
    this.defaultMinimalTime = defaultMinimalTime;
    this.distributedAcker = distributedAcker;
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

  public boolean distributedAcker() {
    return this.distributedAcker;
  }

  @Override
  public String toString() {
    return "CommitterConfig{maxElementsInGraph=" + maxElementsInGraph + '}';
  }
}
