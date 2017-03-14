package com.spbsu.datastream.core.graph;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by marnikitta on 2/6/17.
 */
public final class InPort {
  private final static long OFFSET = 20;
  private final static long RAND_MASK = (1 << OFFSET) - 1;
  private final static ThreadLocalRandom rd = ThreadLocalRandom.current();

  private final long id;
  private final String name;

  public InPort() {
    this("NoName");
  }

  public InPort(final String name) {
    this.name = name;
    this.id = (System.currentTimeMillis() << OFFSET) + (rd.nextInt() & RAND_MASK);
  }

  public long id() {
    return id;
  }

  @Override
  public String toString() {
    if (name.equals("NoName")) {
      return "InPort{" + "id='" + id + '\'' +
              '}';
    } else {
      return "InPort{" + "name='" + name + '\'' +
              '}';
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final InPort inPort = (InPort) o;
    return id == inPort.id &&
            Objects.equals(name, inPort.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name);
  }
}

