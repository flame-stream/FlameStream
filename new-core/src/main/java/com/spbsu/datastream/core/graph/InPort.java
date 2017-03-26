package com.spbsu.datastream.core.graph;

import com.spbsu.datastream.core.HashFunction;

import java.util.Objects;
import java.util.Random;

public final class InPort {
  private final static Random rd = new Random();
  private final static int OFFSET = 64 - 41;
  private final static long RAND_MASK = (1 << OFFSET) - 1;

  private final long id;
  private final HashFunction<?> hashFunction;

  public InPort(final HashFunction<?> function) {
    this.id = (System.currentTimeMillis() << OFFSET) + (rd.nextInt() & RAND_MASK);
    this.hashFunction = function;
  }

  public long id() {
    return id;
  }

  public HashFunction<?> hashFunction() {
    return hashFunction;
  }

  @Override
  public String toString() {
    return "InPort{" + "id=" + id +
            ", hashFunction=" + hashFunction +
            '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final InPort port = (InPort) o;
    return id == port.id &&
            Objects.equals(hashFunction, port.hashFunction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, hashFunction);
  }
}

