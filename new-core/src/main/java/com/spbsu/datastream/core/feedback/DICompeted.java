package com.spbsu.datastream.core.feedback;

public class DICompeted {
  private final long globalTs;

  public DICompeted(final long globalTs) {
    this.globalTs = globalTs;
  }

  public long globalTs() {
    return globalTs;
  }
}
