package com.spbsu.flamestream.core.graph;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.ToIntFunction;

public class InPort {
  private final long id;
  private final ToIntFunction<?> hashFunction;

  public InPort(ToIntFunction<?> function) {
    this.id = ThreadLocalRandom.current().nextLong();
    this.hashFunction = function;
  }

  public long id() {
    return id;
  }

  public ToIntFunction<?> hashFunction() {
    return hashFunction;
  }

  @Override
  public String toString() {
    return "InPort{" + "id=" + id + ", hashFunction=" + hashFunction + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final InPort port = (InPort) o;
    return id == port.id;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(id);
  }
}

