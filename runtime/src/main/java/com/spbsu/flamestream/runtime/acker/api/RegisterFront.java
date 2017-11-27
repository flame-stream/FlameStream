package com.spbsu.flamestream.runtime.acker.api;

public class RegisterFront {
  private final String frontId;

  public RegisterFront(String frontId) {
    this.frontId = frontId;
  }

  public String frontId() {
    return frontId;
  }
}
