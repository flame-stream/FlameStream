package com.spbsu.flamestream.runtime.acker.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class FrontTicket {
  private final String frontId;
  private final String nodeId;
  private final GlobalTime allowedTimestamp;

  public FrontTicket(String frontId, String nodeId, GlobalTime allowedTimestamp) {
    this.frontId = frontId;
    this.nodeId = nodeId;
    this.allowedTimestamp = allowedTimestamp;
  }

  public String nodeId() {
    return nodeId;
  }

  public String frontId() {
    return frontId;
  }

  public GlobalTime allowedTimestamp() {
    return allowedTimestamp;
  }

  @Override
  public String toString() {
    return "FrontTicket{" +
            "frontId='" + frontId + '\'' +
            ", nodeId='" + nodeId + '\'' +
            ", allowedTimestamp=" + allowedTimestamp +
            '}';
  }
}
