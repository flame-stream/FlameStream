package com.spbsu.flamestream.runtime.acker.api;

public class RegisterFront {
  private final String frontId;
  private final String nodeId;

  public RegisterFront(String frontId, String nodeId) {
    this.frontId = frontId;
    this.nodeId = nodeId;
  }

  public String frontId() {
    return frontId;
  }

  public String nodeId() {
    return nodeId;
  }
}
