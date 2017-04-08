package com.spbsu.datastream.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

public final class Trace implements Comparable<Trace> {
  private final int[] trace;

  public Trace() {
    this.trace = new int[0];
  }

  public Trace(final int localTime) {
    this.trace = new int[]{localTime};
  }

  public Trace(final Trace trace, final int newLocalTime) {
    this.trace = Arrays.copyOf(trace.trace, trace.trace.length + 1);
    this.trace[this.trace.length - 1] = newLocalTime;
  }

  @JsonCreator
  private Trace(@JsonProperty("trace") final int[] trace) {
    this.trace = trace;
  }

  @JsonProperty("trace")
  private int[] trace() {
    return this.trace;
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
