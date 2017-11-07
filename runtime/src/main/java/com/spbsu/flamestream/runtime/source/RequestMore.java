package com.spbsu.flamestream.runtime.source;

public final class RequestMore {
  private final int count;

  public RequestMore(int i) {
    this.count = i;
  }

  public int count() {
    return count;
  }
}
