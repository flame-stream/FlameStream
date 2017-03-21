package com.spbsu.datastream.core.graph;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class InPort {
  private final static long OFFSET = 20;
  private final static long RAND_MASK = (1 << OFFSET) - 1;
  private final static ThreadLocalRandom rd = ThreadLocalRandom.current();

  private final long id;

  public InPort() {
    this.id = (System.currentTimeMillis() << OFFSET) + (rd.nextInt() & RAND_MASK);
  }

  public long id() {
    return id;
  }

  @Override
  public String toString() {
    return "InPort{" + "id='" + id + '\'' +
            '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final InPort port = (InPort) o;
    return id == port.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}

