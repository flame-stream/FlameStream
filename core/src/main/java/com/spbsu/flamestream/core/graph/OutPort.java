package com.spbsu.flamestream.core.graph;

import java.util.concurrent.ThreadLocalRandom;

public class OutPort {
  private final long id;

  public OutPort() {
    this.id = ThreadLocalRandom.current().nextLong();
  }

  public long id() {
    return id;
  }

  @Override
  public String toString() {
    return "OutPort{" + "id=" + id + '}';
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
