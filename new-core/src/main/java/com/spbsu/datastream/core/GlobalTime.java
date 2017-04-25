package com.spbsu.datastream.core;

import java.util.Comparator;
import java.util.Objects;

public final class GlobalTime implements Comparable<GlobalTime> {
  public static final GlobalTime INF = new GlobalTime(Long.MAX_VALUE, Integer.MAX_VALUE);

  //Inner representation is a subject for a discussion and/or an optimization

  private final long time;

  private final int initHash;

  public GlobalTime(final long time, final int initHash) {
    this.time = time;
    this.initHash = initHash;
  }

  public long time() {
    return this.time;
  }

  public int initHash() {
    return this.initHash;
  }

  @Override
  public int compareTo(final GlobalTime that) {
    return Comparator.comparingLong(GlobalTime::time)
            .thenComparingInt(GlobalTime::initHash)
            .compare(this, that);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    final GlobalTime that = (GlobalTime) o;
    return this.time == that.time &&
            this.initHash == that.initHash;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.time, this.initHash);
  }

  @Override
  public String toString() {
    return "GlobalTime{" + "time=" + this.time +
            ", initHash=" + this.initHash +
            '}';
  }
}
