package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.core.Front;

public class FrontInstance<T extends Front> {
  private final String id;
  private final Class<T> front;
  private final String[] args;

  public FrontInstance(String id, Class<T> front, String[] args) {
    this.front = front;
    this.id = id;
    this.args = args;
  }

  public String id() {
    return id;
  }

  public Class<T> frontClass() {
    return front;
  }

  public String[] args() {
    return args;
  }
}
