package com.spbsu.datastream.core;

import java.util.Comparator;
import java.util.Objects;

public final class Meta implements Comparable<Meta> {
  private final GlobalTime globalTime;

  private final Trace trace;

  public Meta(GlobalTime globalTime) {
    this.globalTime = globalTime;
    this.trace = Trace.EMPTY_TRACE;
  }

  public Meta(Meta oldMeta, int newLocalTime) {
    this.globalTime = oldMeta.globalTime();
    this.trace = new Trace(oldMeta.trace(), new LocalEvent(newLocalTime));
  }

  public Meta(Meta oldMeta, int newLocalTime, int childId) {
    this.globalTime = oldMeta.globalTime();
    this.trace = new Trace(oldMeta.trace(), new LocalEvent(newLocalTime, childId));
  }

  public GlobalTime globalTime() {
    return this.globalTime;
  }

  public Trace trace() {
    return this.trace;
  }

  public boolean isInvalidatedBy(Meta that) {
    return this.globalTime().equals(that.globalTime()) && this.trace().isInvalidatedBy(that.trace());
  }

  @Override
  public int compareTo(Meta that) {
    return Comparator.comparing(Meta::globalTime)
            .thenComparing(Meta::trace)
            .compare(this, that);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    final Meta meta = (Meta) o;
    return Objects.equals(this.globalTime, meta.globalTime) &&
            Objects.equals(this.trace, meta.trace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.globalTime, this.trace);
  }

  @Override
  public String toString() {
    return "Meta{" + "globalTime=" + this.globalTime +
            ", trace=" + this.trace +
            '}';
  }
}
