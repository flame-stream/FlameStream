package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.core.Front;

public class AttachFront<F extends Front> {
  private final String id;
  private final Class<F> clazz;
  private final String[] args;

  public AttachFront(String id, Class<F> clazz, String[] args) {
    this.clazz = clazz;
    this.id = id;
    this.args = args;
  }

  public String id() {
    return id;
  }

  public Class<F> clazz() {
    return clazz;
  }

  public String[] args() {
    return args;
  }
}
