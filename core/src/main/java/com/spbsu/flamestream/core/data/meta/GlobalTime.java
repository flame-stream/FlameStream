package com.spbsu.flamestream.core.data.meta;

import java.util.Comparator;
import java.util.Objects;

public final class GlobalTime implements Comparable<GlobalTime> {
  public static final GlobalTime MIN = new GlobalTime(Long.MIN_VALUE, EdgeInstance.MIN);
  public static final GlobalTime MAX = new GlobalTime(Long.MAX_VALUE, EdgeInstance.MAX);
  //Inner representation is a subject for a discussion and/or an optimization

  private static final Comparator<GlobalTime> NATURAL_ORDER = Comparator
          .comparingLong(GlobalTime::time)
          .thenComparing(GlobalTime::frontInstance);

  private final long time;
  private final EdgeInstance frontInstance;

  public GlobalTime(long time, EdgeInstance frontInstance) {
    this.time = time;
    this.frontInstance = frontInstance;
  }

  public long time() {
    return time;
  }

  public EdgeInstance frontInstance() {
    return frontInstance;
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
    return time == that.time &&
            Objects.equals(frontInstance, that.frontInstance);
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, frontInstance);
  }

  @Override
  public String toString() {
    return "(" + time + ", " + frontInstance + ')';
  }
}
