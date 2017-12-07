package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.core.data.meta.EdgeInstance;

public class RegisterFront {
  private final EdgeInstance frontInstance;

  public RegisterFront(EdgeInstance frontInstance) {
    this.frontInstance = frontInstance;
  }

  public EdgeInstance frontInstance() {
    return frontInstance;
  }
}
