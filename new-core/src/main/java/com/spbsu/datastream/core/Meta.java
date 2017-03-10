package com.spbsu.datastream.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public final class Meta implements Comparable<Meta> {
  public static final Meta ZERO = new Meta(0);

  public static final Meta INFINITY = new Meta(Integer.MAX_VALUE);

  private final long time;
  private final List<Integer> stagesHashes;

  private Meta() {
    this.time = -1;
    this.stagesHashes = Collections.emptyList();
  }

  public Meta(final Meta old, final int hash) {
    final List<Integer> history = new ArrayList<>(old.stagesHashes());
    history.add(hash);
    this.time = old.time;
    this.stagesHashes = history;
  }

  public Meta(final long time) {
    this.time = time;
    this.stagesHashes = Collections.emptyList();
  }

  public static Meta now() {
    return new Meta(System.currentTimeMillis());
  }

  public int tick() {
    return (int) (time / TimeUnit.SECONDS.toMillis(13));
  }

  @Override
  public int compareTo(final Meta o) {
    return time != o.time ? Long.compare(time, o.time) : Integer.compare(stagesHashes.size(), o.stagesHashes.size());
  }

  public long time() {
    return time;
  }

  public List<Integer> stagesHashes() {
    return stagesHashes;
  }

  @Override
  public String toString() {
    return "Meta{" + "time=" + time +
            ", stagesHashes=" + stagesHashes +
            '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final Meta meta = (Meta) o;
    return time == meta.time &&
            Objects.equals(stagesHashes, meta.stagesHashes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, stagesHashes);
  }
}
