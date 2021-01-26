package com.spbsu.flamestream.core.data.meta;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public class Meta implements Comparable<Meta> {
  public static final Comparator<Meta> NATURAL_ORDER = Comparator
          .comparing(Meta::globalTime)
          .thenComparing(Meta::childIds)
          .thenComparing(Meta::trace)
          .thenComparing(Meta::isTombstone);

  public static final class ChildIds implements Comparable<ChildIds> {
    public static ChildIds EMPTY = new ChildIds();

    private final int[] value;

    private ChildIds() {this.value = new int[0];}

    public ChildIds(ChildIds parent, int child) {
      value = Arrays.copyOf(parent.value, parent.value.length + 1);
      value[value.length - 1] = child;
    }

    public int length() {
      return value.length;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof ChildIds) {
        return Arrays.equals(value, ((ChildIds) obj).value);
      }
      return false;
    }

    @Override
    public String toString() {
      return Arrays.toString(value);
    }

    public boolean arePrefixTo(ChildIds that) {
      return Arrays.mismatch(value, that.value) == value.length;
    }

    public int comparePrefixed(int thisPrefix, ChildIds that, int thatPrefix) {
      return Arrays.compare(value, 0, thisPrefix, that.value, 0, thatPrefix);
    }

    @Override
    public int compareTo(@NotNull ChildIds that) {
      return Arrays.compare(value, that.value);
    }
  }

  private final GlobalTime globalTime;
  private final ChildIds childIds;
  private final long trace;
  private final boolean tombstone;
  private final Labels labels;

  public Meta(GlobalTime globalTime) {
    this.globalTime = globalTime;
    this.childIds = ChildIds.EMPTY;
    this.tombstone = false;
    this.trace = 0;
    labels = Labels.EMPTY;
  }

  public Meta(Meta previous, long physicalId, boolean tombstone) {
    this(previous, physicalId, tombstone, previous.labels);
  }

  public Meta(Meta previous, long physicalId, boolean tombstone, Labels labels) {
    this.globalTime = previous.globalTime();
    this.childIds = previous.childIds;
    this.tombstone = tombstone;
    this.trace = previous.trace ^ physicalId;
    this.labels = labels;
  }

  public Meta(Meta previous, long physicalId, int childId) {
    this.globalTime = previous.globalTime();
    this.childIds = new ChildIds(previous.childIds, childId);
    this.tombstone = previous.tombstone;
    this.trace = previous.trace ^ physicalId;
    this.labels = previous.labels;
  }

  public Meta(Meta previous, long physicalId, int childId, Labels labels) {
    this.globalTime = previous.globalTime();
    this.childIds = new ChildIds(previous.childIds, childId);
    this.tombstone = previous.tombstone;
    this.trace = previous.trace ^ physicalId;
    this.labels = labels;
  }

  public boolean isTombstone() {
    return tombstone;
  }

  public GlobalTime globalTime() {
    return globalTime;
  }

  public long trace() {
    return trace;
  }

  public ChildIds childIds() {
    return childIds;
  }

  public Labels labels() {
    return labels;
  }

  @Override
  public int compareTo(@NotNull Meta that) {
    return NATURAL_ORDER.compare(this, that);
  }

  public boolean isInvalidedBy(Meta that) {
    return !tombstone
            && that.tombstone
            && trace == that.trace
            && globalTime.equals(that.globalTime)
            && childIds.equals(that.childIds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Meta meta = (Meta) o;
    return trace == meta.trace &&
            tombstone == meta.tombstone &&
            Objects.equals(globalTime, meta.globalTime) &&
            Objects.equals(childIds, meta.childIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(globalTime, trace, tombstone, childIds);
  }

  @Override
  public String toString() {
    return "(" + globalTime + ", " + childIds + ", " + trace + (tombstone ? ", tombstone" : "") + ')';
  }
}
