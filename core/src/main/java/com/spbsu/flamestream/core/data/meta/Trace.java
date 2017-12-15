package com.spbsu.flamestream.core.data.meta;

import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.spbsu.flamestream.core.data.meta.LocalEvent.childIdOf;
import static com.spbsu.flamestream.core.data.meta.LocalEvent.localEvent;
import static com.spbsu.flamestream.core.data.meta.LocalEvent.localTimeOf;

class Trace implements Comparable<Trace> {
  static final Trace EMPTY_TRACE = new Trace(new long[0]);

  @VisibleForTesting
  @SuppressWarnings("PackageVisibleField")
  final long[] trace;

  Trace(long[] trace) {
    //noinspection AssignmentOrReturnOfFieldWithMutableType
    this.trace = trace;
  }

  Trace(Trace trace, int localTime, int childId) {
    final long[] newTrace = Arrays.copyOf(trace.trace, trace.trace.length + 1);
    newTrace[newTrace.length - 1] = localEvent(localTime, childId);
    this.trace = newTrace;
  }

  public boolean isInvalidatedBy(Trace trace) {
    for (int i = 0; i < Math.min(this.trace.length, trace.trace.length); ++i) {
      if (this.trace[i] != trace.trace[i]) {
        return localTimeOf(this.trace[i]) < localTimeOf(trace.trace[i]);
      }
    }
    return false;
  }

  @Override
  public int compareTo(Trace trace) {
    for (int i = 0; i < Math.min(this.trace.length, trace.trace.length); ++i) {
      if (this.trace[i] != trace.trace[i]) {
        return Long.compare(this.trace[i], trace.trace[i]);
      }
    }
    return Integer.compare(this.trace.length, trace.trace.length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
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
    return Arrays.stream(trace)
            .mapToObj(event -> localTimeOf(event) + ":" + childIdOf(event))
            .collect(Collectors.joining(", ", "[", "]"));
  }
}
