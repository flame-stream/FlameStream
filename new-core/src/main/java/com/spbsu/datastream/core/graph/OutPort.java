package com.spbsu.datastream.core.graph;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by marnikitta on 2/6/17.
 */
public final class OutPort {
  private final static ThreadLocalRandom rd = ThreadLocalRandom.current();
  private final long id;
  private final String name;

  public OutPort() {
    this("NoName");
  }

  public OutPort(final String name) {
    this.id = rd.nextLong();
    this.name = name;
  }

  public long id() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final OutPort port = (OutPort) o;
    return id == port.id &&
            Objects.equals(name, port.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, name);
  }

  @Override
  public String toString() {
    if (Objects.equals(name, "NoName")) {
      return "OutPort{" + "id='" + id + '\'' +
              '}';
    } else {
      return "OutPort{" + "name='" + name + '\'' +
              '}';
    }
  }
}
