package com.spbsu.datastream.core.meta;

import java.util.Comparator;
import java.util.Objects;

final class MetaImpl implements Meta {
  private final GlobalTime globalTime;

  private final Trace trace;

  MetaImpl(GlobalTime globalTime) {
    this.globalTime = globalTime;
    this.trace = Trace.EMPTY_TRACE;
  }

  MetaImpl(GlobalTime globalTime, Trace trace) {
    this.globalTime = globalTime;
    this.trace = trace;
  }

  @Override
  public Meta advanced(int newLocalTime) {
    return new MetaImpl(globalTime, trace.advanced(newLocalTime, 0));
  }

  @Override
  public Meta advanced(int newLocalTime, int childId) {
    return new MetaImpl(globalTime, trace.advanced(newLocalTime, childId));
  }

  @Override
  public boolean isInvalidatedBy(Meta that) {
    return globalTime.equals(that.globalTime())
            && trace.isInvalidatedBy(that.trace());
  }

  @Override
  public GlobalTime globalTime() {
    return globalTime;
  }

  @Override
  public Trace trace() {
    return trace;
  }

  @Override
  public int compareTo(Meta that) {
    return NATURAL_ORDER.compare(this, that);
  }

  private static final Comparator<Meta> NATURAL_ORDER = Comparator
          .comparing(Meta::globalTime)
          .thenComparing(Meta::trace);

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final MetaImpl meta = (MetaImpl) o;
    return Objects.equals(globalTime, meta.globalTime) &&
            Objects.equals(trace, meta.trace);
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
