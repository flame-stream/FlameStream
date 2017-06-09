package com.spbsu.datastream.core;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

@SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
public final class Trace implements Comparable<Trace> {
  //Inner representation is a subject for a discussion and/or an optimization
  public static final Trace EMPTY_TRACE = new Trace();

  /**
   * Invalidating relation
   */
  public static final Comparator<Trace> INVALIDATION_COMPARATOR = (t0, t1) -> {
    for (int i = 0; i < Math.min(t0.size(), t1.size()); ++i) {
      if (!t0.eventAt(i).equals(t1.eventAt(i))) {
        return Long.compare(t0.eventAt(i).localTime(), t1.eventAt(i).localTime());
      }
    }
    return 0;
  };

  /**
   * Compares traces lexicographically. Invalidate related traces are equal.
   */
  public static final Comparator<Trace> INVALIDATION_IGNORING_COMPARATOR = (t0, t1) -> {
    for (int i = 0; i < Math.min(t0.size(), t1.size()); ++i) {
      final LocalEvent t0Event = t0.eventAt(i);
      final LocalEvent t1Event = t1.eventAt(i);
      if (!Objects.equals(t0Event, t1Event)) {
        if (t0Event.localTime() == t1Event.localTime()) {
          return Integer.compare(t0Event.childId(), t1Event.childId());
        } else {
          return 0;
        }
      }
    }
    return Integer.compare(t0.size(), t1.size());
  };

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

  @Override
  public int compareTo(Trace that) {
    return Trace.INVALIDATION_IGNORING_COMPARATOR
            .thenComparing(Trace.INVALIDATION_COMPARATOR)
            .compare(this, that);
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
