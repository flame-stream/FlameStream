package com.spbsu.flamestream.core.front;

/**
 * User: Artem
 * Date: 26.09.2017
 */
public class TsResponse {
  private final long ts;

  TsResponse(long ts) {
    this.ts = ts;
  }

  public long ts() {
    return ts;
  }
}
