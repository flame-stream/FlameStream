package com.spbsu.flamestream.runtime.source.api;

/**
 * User: Artem
 * Date: 10.11.2017
 */
public class PleaseWait {
  private final long durationMillis;

  public PleaseWait(long durationMillis) {
    this.durationMillis = durationMillis;
  }

  public long durationMillis() {
    return this.durationMillis;
  }
}
