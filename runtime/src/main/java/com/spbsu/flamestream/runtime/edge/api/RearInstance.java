package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.core.Rear;

public class RearInstance<T extends Rear> {
  private final String rearId;
  private final Class<T> rear;
  private final String[] args;

  public RearInstance(String name, Class<T> rear, String[] args) {
    this.rearId = name;
    this.rear = rear;
    this.args = args;
  }

  public String rearId() {
    return rearId;
  }

  public Class<T> rear() {
    return rear;
  }

  public String[] args() {
    return args;
  }
}
