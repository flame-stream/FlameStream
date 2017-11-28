package com.spbsu.flamestream.runtime.acker.api;

public class RegisterFront {
  private final String id;

  public RegisterFront(String id) {
    this.id = id;
  }

  public String id() {
    return id;
  }
}
