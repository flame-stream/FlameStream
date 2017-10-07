package com.spbsu.flamestream.runtime.front;

/**
 * User: Artem
 * Date: 07.10.2017
 */
class TickFrontPing {
  private final long ts;

  TickFrontPing(long ts) {
    this.ts = ts;
  }

  long ts() {
    return ts;
  }
}
