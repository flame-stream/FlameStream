package com.spbsu.datastream.core.graph;

import java.security.SecureRandom;
import java.util.Random;
import java.util.function.ToIntFunction;

public final class InPort {
  private static final Random RANDOM = new SecureRandom();
  private static final long OFFSET = 64L - 41L;
  private static final long RAND_MASK = (1L << InPort.OFFSET) - 1L;

  private final long id;
  private final ToIntFunction<?> hashFunction;

  public InPort(ToIntFunction<?> function) {
    this.id = (System.currentTimeMillis() << InPort.OFFSET) + (InPort.RANDOM.nextLong() & InPort.RAND_MASK);
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
    return "InPort{" + "id=" + id +
            ", hashFunction=" + hashFunction +
            '}';
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

