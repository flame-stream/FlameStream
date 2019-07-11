package com.spbsu.flamestream.runtime.config;

public class SystemConfig {
  public enum Acking {
    DISABLED,
    CENTRALIZED,
    DISTRIBUTED;
  }

  private final int maxElementsInGraph;
  private final int millisBetweenCommits;
  private final long defaultMinimalTime;
  private final Acking acking;
  private final boolean barrierDisabled;

  public SystemConfig(
          int maxElementsInGraph,
          int millisBetweenCommits,
          long defaultMinimalTime,
          Acking acking,
          boolean barrierDisabled
  ) {
    if (acking == SystemConfig.Acking.DISABLED && !barrierDisabled)
      throw new IllegalArgumentException("barrier should be disabled when acking is");
    this.maxElementsInGraph = maxElementsInGraph;
    this.millisBetweenCommits = millisBetweenCommits;
    this.defaultMinimalTime = defaultMinimalTime;
    this.acking = acking;
    this.barrierDisabled = barrierDisabled;
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

  public Acking acking() {
    return this.acking;
  }

  @Override
  public String toString() {
    return "SystemConfig{maxElementsInGraph=" + maxElementsInGraph + '}';
  }

  public boolean barrierIsDisabled() {
    return barrierDisabled;
  }
}
