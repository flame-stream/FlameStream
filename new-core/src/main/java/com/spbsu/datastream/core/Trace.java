package com.spbsu.datastream.core;

import java.util.Arrays;

@SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
public final class Trace implements Comparable<Trace> {
  //Inner representation is a subject for a discussion and/or an optimization
  public static final Trace EMPTY_TRACE = new Trace();

  private final LocalEvent[] trace;

  private Trace() {
    this.trace = new LocalEvent[0];
  }

  public Trace(LocalEvent localEvent) {
    this.trace = new LocalEvent[]{localEvent};
  }

  public Trace(Trace trace, LocalEvent newLocalEvent) {
    this.trace = Arrays.copyOf(trace.trace, trace.trace.length + 1);
    this.trace[this.trace.length - 1] = newLocalEvent;
  }

  public LocalEvent eventAt(int position) {
    return this.trace[position];
  }

  public int size() {
    return this.trace.length;
  }

  public boolean isInvalidatedBy(Trace that) {
    for (int i = 0; i < Math.min(this.size(), that.size()); ++i) {
      if (!this.eventAt(i).equals(that.eventAt(i))) {
        return this.eventAt(i).localTime() < that.eventAt(i).localTime();
      }
    }
    return false;
  }

  @Override
  public int compareTo(Trace that) {
    for (int i = 0; i < Math.min(that.trace.length, this.trace.length); ++i) {

      final int compare = this.eventAt(i).compareTo(that.eventAt(i));
      if (compare != 0) {
        return compare;
      }
    }

    return Integer.compare(this.trace.length, that.trace.length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    final Trace trace1 = (Trace) o;
    return Arrays.equals(this.trace, trace1.trace);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.trace);
  }

  @Override
  public String toString() {
    return Arrays.toString(this.trace);
  }
}
