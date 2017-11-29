package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.core.Front;

public class FrontInstance<T extends Front> {
  private final String name;
  private final Class<T> front;
  private final String[] args;

  public FrontInstance(String name, Class<T> front, String[] args) {
    this.front = front;
    this.name = name;
    this.args = args;
  }

  public String name() {
    return name;
  }

  public Class<T> front() {
    return front;
  }

  public String[] args() {
    return args;
  }
}
