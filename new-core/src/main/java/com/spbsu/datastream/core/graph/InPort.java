package com.spbsu.datastream.core.graph;

import java.util.Objects;

/**
 * Created by marnikitta on 2/6/17.
 */
public final class InPort {
  private static int counter;

  private final int id;

  public InPort() {
    this.id = counter++;
  }

  public InPort(final int id) {
    this.id = id;
  }

  public int id() {
    return id;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final InPort inPort = (InPort) o;
    return id == inPort.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
