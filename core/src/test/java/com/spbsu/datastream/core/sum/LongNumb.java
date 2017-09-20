package com.spbsu.datastream.core.sum;

import java.util.Objects;

public final class LongNumb implements Numb {
  private final long value;

  public LongNumb(long value) {
    this.value = value;
  }

  @Override
  public long value() {
    return value;
  }

  @Override
  public String toString() {
    return "LongNumb{" + "value=" + value +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final LongNumb longNumb = (LongNumb) o;
    return value == longNumb.value;
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
