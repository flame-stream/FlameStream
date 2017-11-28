package com.spbsu.flamestream.runtime.node.graph.acker.api;

public class RegisterFront {
  private final String id;

  public RegisterFront(String id) {
    this.id = id;
  }

  public String id() {
    return id;
  }
}
