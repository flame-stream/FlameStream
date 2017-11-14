package com.spbsu.flamestream.core.data.meta;

import java.util.Comparator;
import java.util.Objects;

public final class GlobalTime implements Comparable<GlobalTime> {
  public static final GlobalTime MIN = new GlobalTime(Long.MIN_VALUE, "min");
  //Inner representation is a subject for a discussion and/or an optimization

  private static final Comparator<GlobalTime> NATURAL_ORDER = Comparator.comparingLong(GlobalTime::time)
          .thenComparing(GlobalTime::front);

  private final long time;

  private final String front;

  public GlobalTime(long time, String front) {
    this.time = time;
    this.front = front;
  }

  public long time() {
    return time;
  }

  public String front() {
    return front;
  }

  @Override
  public int compareTo(GlobalTime that) {
    return NATURAL_ORDER.compare(this, that);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final GlobalTime that = (GlobalTime) o;
    return (time == that.time) && (front.equals(that.front));
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, front);
  }

  @Override
  public String toString() {
    return "(" + time + ':' + front + ')';
  }
}
