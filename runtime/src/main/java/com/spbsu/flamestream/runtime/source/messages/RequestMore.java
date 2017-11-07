package com.spbsu.flamestream.runtime.source.messages;

public final class RequestMore {
  private final int count;

  public RequestMore(int i) {
    this.count = i;
  }

  public int count() {
    return count;
  }
}
