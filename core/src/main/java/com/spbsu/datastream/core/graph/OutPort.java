package com.spbsu.datastream.core.graph;

import java.security.SecureRandom;
import java.util.Random;

public final class OutPort {
  private static final Random RANDOM = new SecureRandom();
  private static final long OFFSET = 64L - 41L;
  private static final long RAND_MASK = (1L << OutPort.OFFSET) - 1L;
  private final long id;

  public OutPort() {
    this.id = (System.currentTimeMillis() << OutPort.OFFSET) + (OutPort.RANDOM.nextLong() & OutPort.RAND_MASK);
  }


  public long id() {
    return id;
  }

  @Override
  public String toString() {
    return "OutPort{" + "id=" + id +
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
    final OutPort port = (OutPort) o;
    return id == port.id;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(id);
  }
}
