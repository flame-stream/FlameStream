package com.spbsu.flamestream.core.data.meta;

import java.util.Comparator;
import java.util.Objects;

public final class GlobalTime implements Comparable<GlobalTime> {
  public static final GlobalTime MIN = new GlobalTime(Long.MIN_VALUE, "min", "min");
  public static final GlobalTime MAX = new GlobalTime(Long.MAX_VALUE, "max", "max");
  //Inner representation is a subject for a discussion and/or an optimization

  private static final Comparator<GlobalTime> NATURAL_ORDER = Comparator.comparingLong(GlobalTime::time)
          .thenComparing(GlobalTime::frontId)
          .thenComparing(GlobalTime::nodeId);

  private final long time;

  private final String frontId;

  private final String nodeId;

  public GlobalTime(long time, String frontId, String nodeId) {
    this.time = time;
    this.frontId = frontId;
    this.nodeId = nodeId;
  }

  public long time() {
    return time;
  }

  public String frontId() {
    return frontId;
  }

  public String nodeId() {
    return nodeId;
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
            Objects.equals(frontId, that.frontId) &&
            Objects.equals(nodeId, that.nodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, frontId, nodeId);
  }

  @Override
  public String toString() {
    return "(" + time + ", " + frontId + ", " + nodeId + ')';
  }
}
