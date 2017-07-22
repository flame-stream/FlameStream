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

  private Meta(GlobalTime globalTime, Trace trace) {
    this.globalTime = globalTime;
    this.trace = trace;
  }

  public Meta advanced(int newLocalTime) {
    return new Meta(this.globalTime, new Trace(this.trace, new LocalEvent(newLocalTime)));
  }

  public Meta advanced(int newLocalTime, int child) {
    return new Meta(this.globalTime, new Trace(this.trace, new LocalEvent(newLocalTime, child)));
  }

  public GlobalTime globalTime() {
    return this.globalTime;
  }

  public Trace trace() {
    return this.trace;
  }

  @Override
  public int compareTo(Meta that) {
    return Meta.NATURAL_ORDER.compare(this, that);
  }

  public static final Comparator<Meta> NATURAL_ORDER = Comparator
          .comparing(Meta::globalTime)
          .thenComparing(Meta::trace);

  public boolean isBrother(Meta that) {
    if (this.globalTime.equals(that.globalTime())
            && Trace.INVALIDATION_COMPARATOR.compare(this.trace, that.trace()) == 0) {
      return true;
    } else {
      return false;
    }
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
