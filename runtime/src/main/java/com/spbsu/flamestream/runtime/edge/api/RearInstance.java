package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.core.Rear;

public class RearInstance<T extends Rear> {
  private final String id;
  private final Class<T> rearClass;
  private final String[] args;

  public RearInstance(String id, Class<T> rearClass, String[] args) {
    this.id = id;
    this.rearClass = rearClass;
    this.args = args;
  }

  public String id() {
    return id;
  }

  public Class<T> rearClass() {
    return rearClass;
  }

  public String[] args() {
    return args;
  }
}
