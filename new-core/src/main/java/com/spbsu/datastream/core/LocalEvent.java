package com.spbsu.datastream.core;

import java.util.Comparator;
import java.util.Objects;

public final class LocalEvent implements Comparable<LocalEvent> {
  //Inner representation is a subject for a discussion and/or an optimization

  private final int localTime;

  private final int childId;

  public LocalEvent(int localTime) {
    this.localTime = localTime;
    this.childId = 0;
  }

  public LocalEvent(int localTime, int childId) {
    this.localTime = localTime;
    this.childId = childId;
  }

  public int localTime() {
    return this.localTime;
  }

  public int childId() {
    return this.childId;
  }

  @Override
  public int compareTo(LocalEvent that) {
    return Comparator.comparingInt(LocalEvent::localTime)
            .thenComparingInt(LocalEvent::childId)
            .compare(this, that);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    final LocalEvent that = (LocalEvent) o;
    return this.localTime == that.localTime &&
            this.childId == that.childId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.localTime, this.childId);
  }

  @Override
  public String toString() {
    return "LocalEvent{" + "localTime=" + this.localTime +
            ", childId=" + this.childId +
            '}';
  }
}
