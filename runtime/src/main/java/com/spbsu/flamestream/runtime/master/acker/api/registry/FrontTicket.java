package com.spbsu.flamestream.runtime.master.acker.api.registry;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class FrontTicket {
  private final GlobalTime allowedTimestamp;

  public FrontTicket(GlobalTime allowedTimestamp) {
    this.allowedTimestamp = allowedTimestamp;
  }

  public GlobalTime allowedTimestamp() {
    return allowedTimestamp;
  }

  @Override
  public String toString() {
    return "FrontTicket{" +
            "allowedTimestamp=" + allowedTimestamp +
            '}';
  }
}
