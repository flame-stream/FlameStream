package com.spbsu.datastream.core.graph;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public final class OutPort {
  private final static long OFFSET = 20;
  private final static long RAND_MASK = (1 << OFFSET) - 1;
  private final static ThreadLocalRandom rd = ThreadLocalRandom.current();
  private final long id;

  public OutPort() {
    this.id = (System.currentTimeMillis() << OFFSET) + (rd.nextInt() & RAND_MASK);
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
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final OutPort port = (OutPort) o;
    return id == port.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
