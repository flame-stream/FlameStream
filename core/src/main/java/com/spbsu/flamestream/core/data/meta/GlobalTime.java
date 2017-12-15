package com.spbsu.flamestream.core.data.meta;

import java.util.Comparator;
import java.util.Objects;

public final class GlobalTime implements Comparable<GlobalTime> {
  public static final GlobalTime MIN = new GlobalTime(Long.MIN_VALUE, EdgeId.MIN);
  public static final GlobalTime MAX = new GlobalTime(Long.MAX_VALUE, EdgeId.MAX);
  //Inner representation is a subject for a discussion and/or an optimization

  private static final Comparator<GlobalTime> NATURAL_ORDER = Comparator
          .comparingLong(GlobalTime::time)
          .thenComparing(GlobalTime::frontId);

  private final long time;
  private final EdgeId frontId;

  public GlobalTime(long time, EdgeId frontId) {
    this.time = time;
    this.frontId = frontId;
  }

  public long time() {
    return time;
  }

  public EdgeId frontId() {
    return frontId;
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
            Objects.equals(frontId, that.frontId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, frontId);
  }

  @Override
  public String toString() {
    return "(" + time + ", " + frontId + ')';
  }
}
