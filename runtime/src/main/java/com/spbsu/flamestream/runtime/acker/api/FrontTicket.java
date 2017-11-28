package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class FrontTicket {
  private final String frontId;
  private final GlobalTime allowedTimestamp;

  public FrontTicket(String frontId, GlobalTime allowedTimestamp) {
    this.frontId = frontId;
    this.allowedTimestamp = allowedTimestamp;
  }

  public String frontId() {
    return frontId;
  }

  public GlobalTime allowedTimestamp() {
    return allowedTimestamp;
  }
}
