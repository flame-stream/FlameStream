package com.spbsu.datastream.core;

import java.util.Comparator;
import java.util.Objects;

public final class GlobalTime implements Comparable<GlobalTime> {
  public static final GlobalTime INF = new GlobalTime(Long.MAX_VALUE, -1);

  //Inner representation is a subject for a discussion and/or an optimization

  private final long time;

  private final int front;

  public GlobalTime(final long time, final int front) {
    this.time = time;
    this.front = front;
  }

  public long time() {
    return this.time;
  }

  public int front() {
    return this.front;
  }

  @Override
  public int compareTo(final GlobalTime that) {
    return Comparator.comparingLong(GlobalTime::time)
            .thenComparingInt(GlobalTime::front)
            .compare(this, that);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || this.getClass() != o.getClass()) return false;
    final GlobalTime that = (GlobalTime) o;
    return this.time == that.time &&
            Objects.equals(this.front, that.front);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.time, this.front);
  }

  @Override
  public String toString() {
    return "GlobalTime{" + "time=" + this.time +
            ", front='" + this.front + '\'' +
            '}';
  }
}
