package com.spbsu.flamestream.runtime.node.edge.api;

import com.spbsu.flamestream.core.Rear;

import java.util.List;

public class RearInstance<T extends Rear> {
  private final String rearId;
  private final Class<T> rear;
  private final List<Object> params;

  public RearInstance(String rearId, Class<T> rear, List<Object> params) {
    this.rearId = rearId;
    this.rear = rear;
    this.params = params;
  }

  public String rearId() {
    return rearId;
  }

  public Class<T> rear() {
    return rear;
  }

  public List<Object> params() {
    return params;
  }
}
