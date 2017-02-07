package com.spbsu.datastream.core.graph;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by marnikitta on 2/6/17.
 */
public final class OutPort {
  private final static ThreadLocalRandom rd = ThreadLocalRandom.current();
  private final long id;

  public OutPort() {
    this.id = System.currentTimeMillis() << 5 + rd.nextInt(1 << 5);
  }

  public long id() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final OutPort outPort = (OutPort) o;
    return id == outPort.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("OutPort{");
    sb.append("id=").append(id);
    sb.append('}');
    return sb.toString();
  }
}
