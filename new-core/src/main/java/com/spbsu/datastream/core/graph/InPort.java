package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.HashFunction;

import java.security.SecureRandom;
import java.util.Objects;
import java.util.Random;

public final class InPort {
  private static final Random RANDOM = new SecureRandom();
  private static final long OFFSET = 64L - 41L;
  private static final long RAND_MASK =  (1L << InPort.OFFSET) - 1L;

  private final long id;
  private final HashFunction<?> hashFunction;

  public InPort(final HashFunction<?> function) {
    this.id = (System.currentTimeMillis() << InPort.OFFSET) + (InPort.RANDOM.nextLong() & InPort.RAND_MASK);
    this.hashFunction = function;
  }

  public long id() {
    return this.id;
  }

  public HashFunction<?> hashFunction() {
    return this.hashFunction;
  }

  @Override
  public String toString() {
    return "InPort{" + "id=" + this.id +
            ", hashFunction=" + this.hashFunction +
            '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    final InPort port = (InPort) o;
    return this.id == port.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.id);
  }
}

