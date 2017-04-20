package com.spbsu.datastream.core;

import java.util.Comparator;
import java.util.Objects;

public final class Meta implements Comparable<Meta> {
  private final GlobalTime globalTime;

  private final Trace trace;

  public Meta(final GlobalTime globalTime, final int localTime) {
    this.globalTime = globalTime;
    this.trace = new Trace(new LocalEvent(localTime));
  }

  public Meta(final Meta oldMeta, final int newLocalTime) {
    this.globalTime = oldMeta.globalTime();
    this.trace = new Trace(oldMeta.trace(), new LocalEvent(newLocalTime));
  }

  public Meta(final Meta oldMeta, final int newLocalTime, final int childId) {
    this.globalTime = oldMeta.globalTime();
    this.trace = new Trace(oldMeta.trace(), new LocalEvent(newLocalTime, childId));
  }

  public GlobalTime globalTime() {
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
    return Comparator.comparing(Meta::globalTime)
            .thenComparing(Meta::trace)
            .compare(this, that);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    final Meta meta = (Meta) o;
    return Objects.equals(globalTime, meta.globalTime) &&
            Objects.equals(trace, meta.trace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(globalTime, trace);
  }

  @Override
  public String toString() {
    return "Meta{" + "globalTime=" + globalTime +
            ", trace=" + trace +
            '}';
  }
}
