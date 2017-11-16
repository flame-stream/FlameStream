package com.spbsu.flamestream.core.data.meta;

import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.spbsu.flamestream.core.data.meta.LocalEvent.childIdOf;
import static com.spbsu.flamestream.core.data.meta.LocalEvent.localEvent;
import static com.spbsu.flamestream.core.data.meta.LocalEvent.localTimeOf;

class TraceImpl implements Trace {
  @VisibleForTesting
  @SuppressWarnings("PackageVisibleField")
  final long[] trace;

  TraceImpl(long[] trace) {
    //noinspection AssignmentOrReturnOfFieldWithMutableType
    this.trace = trace;
  }

  @Override
  public TraceImpl advanced(int localTime, int childId) {
    final long[] newTrace = Arrays.copyOf(trace, trace.length + 1);
    newTrace[newTrace.length - 1] = localEvent(localTime, childId);
    return new TraceImpl(newTrace);
  }

  @Override
  public boolean isInvalidatedBy(Trace trace) {
    final TraceImpl that = (TraceImpl) trace;
    for (int i = 0; i < Math.min(this.trace.length, that.trace.length); ++i) {
      if (this.trace[i] != that.trace[i]) {
        return localTimeOf(this.trace[i]) < localTimeOf(that.trace[i]);
      }
    }
    return false;
  }

  @Override
  public int compareTo(Trace trace) {
    final TraceImpl that = (TraceImpl) trace;
    for (int i = 0; i < Math.min(this.trace.length, that.trace.length); ++i) {
      if (this.trace[i] != that.trace[i]) {
        return Long.compare(this.trace[i], that.trace[i]);
      }
    }
    return Integer.compare(this.trace.length, that.trace.length);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final TraceImpl trace1 = (TraceImpl) o;
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
