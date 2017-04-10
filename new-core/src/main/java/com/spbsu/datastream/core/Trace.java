package com.spbsu.datastream.core;

import java.util.Arrays;

public final class Trace implements Comparable<Trace> {
  private final int[] trace;

  private final int[] splitPoints;

  public Trace(final int localTime) {
    this.trace = new int[]{localTime};
    this.splitPoints = new int[0];
  }

  public Trace(final Trace trace, final int newLocalTime, final boolean isSplitPoint) {
    this.trace = Arrays.copyOf(trace.trace, trace.trace.length + 1);
    this.trace[this.trace.length - 1] = newLocalTime;
    if (isSplitPoint) {
      this.splitPoints = Arrays.copyOf(trace.splitPoints, trace.splitPoints.length + 1);
      this.splitPoints[this.splitPoints.length - 1] = this.trace.length - 1;
    } else {
      this.splitPoints = trace.splitPoints;
    }
  }

  public int timeAt(final int position) {
    return trace[position];
  }

  public boolean wasSplitAt(final int position) {
    return Arrays.stream(splitPoints).anyMatch(splitPoint -> splitPoint == position);
  }

  @Override
  public int compareTo(final Trace that) {
    for (int i = 0; i < Math.min(that.trace.length, this.trace.length); ++i) {
      if (this.trace[i] < that.trace[i]) {
        return -1;
      } else if (this.trace[i] > that.trace[i]) {
        return 1;
      }
    }

    return Integer.compare(this.trace.length, that.trace.length);
  }

  @Override
  public String toString() {
    return "Trace{" + "trace=" + Arrays.toString(trace) +
            '}';
  }
}
