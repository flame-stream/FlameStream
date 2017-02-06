package com.spbsu.datastream.core.graph;

import java.util.Objects;

/**
 * Created by marnikitta on 2/6/17.
 */
public final class OutPort {
  private static int counter;

  private final int id;

  public OutPort() {
    this.id = counter++;
  }

  public OutPort(final int id) {
    this.id = id;
  }

  public int id() {
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
}
