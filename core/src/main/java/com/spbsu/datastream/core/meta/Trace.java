package com.spbsu.datastream.core.meta;

import java.util.Comparator;
import java.util.Objects;

import static com.spbsu.datastream.core.meta.LocalEvent.childIdOf;
import static com.spbsu.datastream.core.meta.LocalEvent.localTimeOf;

public interface Trace extends Comparable<Trace> {
  Trace advanced(int localTime, int childId);

  Trace EMPTY_TRACE = new TraceImpl(new long[0]);

  /**
   * Invalidating relation
   */
  Comparator<Trace> INVALIDATION_COMPARATOR = (trace0, trace1) -> {
    final TraceImpl t0 = (TraceImpl) trace0;
    final TraceImpl t1 = (TraceImpl) trace1;

    for (int i = 0; i < Math.min(t0.trace.length, t1.trace.length); ++i) {
      if (t0.trace[i] != t1.trace[i]) {
        return Long.compare(localTimeOf(t0.trace[i]), localTimeOf(t1.trace[i]));
      }
    }
    return Integer.compare(t0.trace.length, t1.trace.length);
  };

  /**
   * Compares traces lexicographically. Invalidate related traces are equal.
   */
  Comparator<Trace> INVALIDATION_IGNORING_COMPARATOR = (trace0, trace1) -> {
    final TraceImpl t0 = (TraceImpl) trace0;
    final TraceImpl t1 = (TraceImpl) trace1;

    for (int i = 0; i < Math.min(t0.trace.length, t1.trace.length); ++i) {
      final long t0Event = t0.trace[i];
      final long t1Event = t1.trace[i];
      if (!Objects.equals(t0Event, t1Event)) {
        if (localTimeOf(t0Event) == localTimeOf(t1Event)) {
          return Integer.compare(childIdOf(t0Event), childIdOf(t1Event));
        } else {
          return 0;
        }
      }
    }
    return Integer.compare(t0.trace.length, t1.trace.length);
  };
}
