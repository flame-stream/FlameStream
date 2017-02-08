package com.spbsu.datastream.core.graph;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by marnikitta on 2/6/17.
 */
public final class InPort {
  private final static ThreadLocalRandom rd = ThreadLocalRandom.current();
  private final long id;
  private final String name;

  public InPort() {
    this("NoName");
  }

  public InPort(final String name) {
    this.name = name;
    this.id = System.currentTimeMillis() << 3 + rd.nextInt(1 << 3);
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
}

