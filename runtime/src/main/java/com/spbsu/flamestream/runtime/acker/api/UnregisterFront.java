package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.core.data.meta.EdgeId;

public class UnregisterFront {
  private final EdgeId frontId;

  public UnregisterFront(EdgeId frontId) {
    this.frontId = frontId;
  }

  public EdgeId frontId() {
    return frontId;
  }
}
