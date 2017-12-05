package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.core.Rear;

public class RearInstance<R extends Rear> {
  private final String id;
  private final Class<R> clazz;
  private final String[] args;

  public RearInstance(String id, Class<R> clazz, String[] args) {
    this.id = id;
    this.clazz = clazz;
    this.args = args;
  }

  public String id() {
    return id;
  }

  public Class<R> clazz() {
    return clazz;
  }

  public String[] args() {
    return args;
  }
}
