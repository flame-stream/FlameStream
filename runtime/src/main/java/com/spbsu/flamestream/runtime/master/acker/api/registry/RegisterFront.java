package com.spbsu.flamestream.runtime.master.acker.api.registry;

import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.master.acker.api.AckerInputMessage;

public class RegisterFront implements AckerInputMessage {
  private final EdgeId frontId;

  public RegisterFront(EdgeId frontId) {
    this.frontId = frontId;
  }

  public EdgeId frontId() {
    return frontId;
  }
}
