package com.spbsu.datastream.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

public final class Meta implements Comparable<Meta> {
  private final long globalTime;

  private final Trace trace;

  public Meta(final long globalTime) {
    this.globalTime = globalTime;
    this.trace = new Trace();
  }

  public Meta(final Meta oldMeta, final int newLocalTime) {
    this.globalTime = oldMeta.globalTime();
    this.trace = new Trace(oldMeta.trace(), newLocalTime);
  }

  @JsonCreator
  private Meta(@JsonProperty("trace") final Trace trace, @JsonProperty("globalTime") final long globalTime) {
    this.globalTime = globalTime;
    this.trace = trace;
  }

  public static Meta now() {
    return new Meta(System.currentTimeMillis());
  }

  @JsonProperty("globalTime")
  public long globalTime() {
    return globalTime;
  }

  @JsonProperty("localTime")
  public Trace trace() {
    return trace;
  }

  @JsonIgnore
  public int tick() {
    return (int) (globalTime() / TimeUnit.HOURS.toMillis(1));
  }

  @Override
  public int compareTo(final Meta that) {
    return Comparator.comparingLong(Meta::globalTime)
            .thenComparing(Meta::trace)
            .compare(this, that);
  }

  @Override
  public String toString() {
    return "Meta{" + "globalTime=" + globalTime +
            ", trace=" + trace +
            '}';
  }
}
