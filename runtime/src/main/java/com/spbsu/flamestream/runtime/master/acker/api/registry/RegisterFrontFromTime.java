package com.spbsu.flamestream.runtime.master.acker.api.registry;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.master.acker.api.AckerInputMessage;

public class RegisterFrontFromTime implements AckerInputMessage {
  public final GlobalTime startTime;

  public RegisterFrontFromTime(GlobalTime startTime) {
    this.startTime = startTime;
  }
}
