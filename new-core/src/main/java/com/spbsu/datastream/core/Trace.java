package com.spbsu.datastream.core;

import java.util.Arrays;

public final class Trace implements Comparable<Trace> {
  //Inner representation is a subject for a discussion and/or an optimization

  private final LocalEvent[] trace;

  public Trace(final LocalEvent localEvent) {
    this.trace = new LocalEvent[]{localEvent};
  }

  public Trace(final Trace trace, final LocalEvent newLocalEvent) {
    this.trace = Arrays.copyOf(trace.trace, trace.trace.length + 1);
    this.trace[this.trace.length - 1] = newLocalEvent;
  }

  public LocalEvent eventAt(final int position) {
    return trace[position];
  }

  @Override
  public int compareTo(final Trace that) {
    for (int i = 0; i < Math.min(that.trace.length, this.trace.length); ++i) {

      final int compare = this.eventAt(i).compareTo(that.eventAt(i));
      if (compare != 0) {
        return compare;
      }
    }

    return Integer.compare(this.trace.length, that.trace.length);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    final Trace trace1 = (Trace) o;
    return Arrays.equals(trace, trace1.trace);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(trace);
  }

  @Override
  public String toString() {
    return "Trace{" + "trace=" + Arrays.toString(trace) +
            '}';
  }
}
