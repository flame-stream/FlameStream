package com.spbsu.flamestream.runtime.front;

/**
 * User: Artem
 * Date: 07.10.2017
 */
class TickFrontStopped {
  private final long startTickTs;

  TickFrontStopped(long startTickTs) {
    this.startTickTs = startTickTs;
  }

  long startTickTs() {
    return startTickTs;
  }
}
