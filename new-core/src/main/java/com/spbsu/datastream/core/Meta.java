package com.spbsu.datastream.core;

import java.util.Comparator;

public final class Meta implements Comparable<Meta> {
  private final long globalTime;

  private final int childId;

  private final Trace trace;

  private final HashRange rootRange;

  public Meta(final long globalTime, final HashRange rootRange) {
    this.globalTime = globalTime;
    this.rootRange = rootRange;
    this.childId = 0;
    this.trace = new Trace();
  }

  public Meta(final Meta oldMeta, final int childId, final int newLocalTime) {
    this.rootRange = oldMeta.rootRange();
    this.globalTime = oldMeta.globalTime();
    this.childId = childId;
    this.trace = new Trace(oldMeta.trace(), newLocalTime);
  }

  /**
   * Range of the rootNode, on which this DI was produced
   */
  public HashRange rootRange() {
    return rootRange;
  }

  public long globalTime() {
    return globalTime;
  }

  public Trace trace() {
    return trace;
  }

  /**
   * If DI produces several DI, they marked with different childId
   */
  public int childId() {
    return childId;
  }

  public int tick() {
    return 1;
  }

  @Override
  public int compareTo(final Meta that) {
    return Comparator.comparingLong(Meta::globalTime)
            .thenComparingInt(Meta::childId)
            .thenComparing(Meta::trace)
            .compare(this, that);
  }

  @Override
  public String toString() {
    return "Meta{" + "globalTime=" + globalTime +
            ", childId=" + childId +
            ", trace=" + trace +
            '}';
  }
}
