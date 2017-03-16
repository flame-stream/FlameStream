package com.spbsu.datastream.core.hashable;

import com.spbsu.datastream.core.Hashable;

public class HashableInteger extends Number implements Hashable<HashableInteger> {
  private final int value;

  public HashableInteger(final int value) {
    this.value = value;
  }

  @Override
  public int hash() {
    return value;
  }

  @Override
  public boolean hashEquals(final HashableInteger that) {
    return value == that.value;
  }

  @Override
  public int intValue() {
    return value;
  }

  @Override
  public long longValue() {
    return value;
  }

  @Override
  public float floatValue() {
    return value;
  }

  @Override
  public double doubleValue() {
    return value;
  }

  @Override
  public String toString() {
    return Integer.toString(value);
  }
}
