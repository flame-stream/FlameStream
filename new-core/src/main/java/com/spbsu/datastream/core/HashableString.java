package com.spbsu.datastream.core;

import java.util.Objects;

public class HashableString implements Hashable<HashableString> {
  private final String value;

  public HashableString(final String value) {
    this.value = value;
  }

  @Override
  public int hash() {
    return value.hashCode();
  }

  @Override
  public boolean hashEquals(final HashableString that) {
    return this.value().equals(that.value());
  }

  public String value() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final HashableString that = (HashableString) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }

  @Override
  public String toString() {
    return value();
  }
}