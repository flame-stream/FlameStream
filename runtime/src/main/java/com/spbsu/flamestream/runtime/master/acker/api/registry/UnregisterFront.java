package com.spbsu.flamestream.runtime.master.acker.api.registry;

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
