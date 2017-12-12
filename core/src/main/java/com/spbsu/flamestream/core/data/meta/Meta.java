package com.spbsu.flamestream.core.data.meta;

import java.util.Comparator;
import java.util.Objects;

public class Meta implements Comparable<Meta> {
  private static final Comparator<Meta> NATURAL_ORDER = Comparator
          .comparing(Meta::globalTime)
          .thenComparing(Meta::trace);
  private final GlobalTime globalTime;
  private final Trace trace;

  public Meta(GlobalTime globalTime) {
    this.globalTime = globalTime;
    this.trace = Trace.EMPTY_TRACE;
  }

  public Meta(Meta previous, int localTime) {
    this.globalTime = previous.globalTime();
    this.trace = new Trace(previous.trace(), localTime, 0);
  }

  public Meta(Meta previous, int localTime, int childId) {
    this.globalTime = previous.globalTime();
    this.trace = new Trace(previous.trace(), localTime, childId);
  }

  public boolean isInvalidatedBy(Meta that) {
    return globalTime.equals(that.globalTime()) && trace.isInvalidatedBy(that.trace());
  }

  public GlobalTime globalTime() {
    return globalTime;
  }

  Trace trace() {
    return trace;
  }

  @Override
  public int compareTo(Meta that) {
    return NATURAL_ORDER.compare(this, that);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Meta meta = (Meta) o;
    return Objects.equals(globalTime, meta.globalTime) && Objects.equals(trace, meta.trace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(globalTime, trace);
  }

  @Override
  public String toString() {
    return "(" + globalTime + ", " + trace + ')';
  }
}
