package com.spbsu.flamestream.core.data.meta;

import com.spbsu.flamestream.core.data.meta.impl.TraceImpl;

public interface Trace extends Comparable<Trace> {
  Trace EMPTY_TRACE = new TraceImpl(new long[0]);

  Trace advanced(int localTime, int childId);

  boolean isInvalidatedBy(Trace that);
}
