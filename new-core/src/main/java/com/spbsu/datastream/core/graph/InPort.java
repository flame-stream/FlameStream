package com.spbsu.datastream.core.graph;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by marnikitta on 2/6/17.
 */
public final class InPort {
  private final static ThreadLocalRandom rd = ThreadLocalRandom.current();
  private final long id;

  public InPort() {
    this.id = System.currentTimeMillis() << 5 + rd.nextInt(1 << 5);
  }

  public InPort(final long id) {
    this.id = id;
  }

  public long id() {
    return id;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("InPort{");
    sb.append("id=").append(id);
    sb.append('}');
    return sb.toString();
  }
}

