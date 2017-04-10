package com.spbsu.datastream.core;

import java.util.Comparator;

public final class Meta implements Comparable<Meta> {
  private final long globalTime;

  private final Trace trace;

  private final int rootHash;

  public Meta(final long globalTime, final int localTime, final int rootHash) {
    this.globalTime = globalTime;
    this.rootHash = rootHash;
    this.trace = new Trace(localTime);
  }

  public Meta(final Meta oldMeta, final int newLocalTime, final boolean isSplit) {
    this.rootHash = oldMeta.rootHash();
    this.globalTime = oldMeta.globalTime();
    this.trace = new Trace(oldMeta.trace(), newLocalTime, isSplit);
  }

  /**
   * Hash of the rootNode, on which this DI was produced
   */
  public int rootHash() {
    return rootHash;
  }

  public long globalTime() {
    return globalTime;
  }

  public Trace trace() {
    return trace;
  }

  public int tick() {
    return 1;
  }

  @Override
  public int compareTo(final Meta that) {
    return Comparator.comparingLong(Meta::globalTime)
            .thenComparing(Meta::trace)
            .compare(this, that);
  }

  @Override
  public String toString() {
    return "Meta{" + "globalTime=" + globalTime +
            ", trace=" + trace +
            ", rootHash=" + rootHash +
            '}';
  }
}
