package com.spbsu.flamestream.runtime.node.edge.api;

import com.spbsu.flamestream.core.Front;

import java.util.ArrayList;
import java.util.List;

public class FrontInstance<T extends Front> {
  private final String frontId;
  private final Class<T> front;
  private final List<Object> params;

  public FrontInstance(String frontId, Class<T> front, List<Object> params) {
    this.front = front;
    this.frontId = frontId;
    this.params = new ArrayList<>(params);
  }

  public String frontId() {
    return frontId;
  }

  public Class<T> front() {
    return front;
  }

  public List<Object> params() {
    return params;
  }
}
