package com.spbsu.flamestream.runtime.master.acker.api.registry;

import com.spbsu.flamestream.core.data.meta.EdgeId;

public class RegisterFront {
  private final EdgeId frontId;

  public RegisterFront(EdgeId frontId) {
    this.frontId = frontId;
  }

  public EdgeId frontId() {
    return frontId;
  }
}
