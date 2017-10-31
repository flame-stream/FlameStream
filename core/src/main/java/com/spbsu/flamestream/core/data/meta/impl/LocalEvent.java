package com.spbsu.flamestream.core.data.meta.impl;

/**
 * Wrapper class for localEvents
 * Local events are represented as longs, all actions with localEvents should be done with this interfaces
 */
@SuppressWarnings("UtilityClass")
final class LocalEvent {

  public static final long LOW_QUADWORD = 0xffffffffL;

  private LocalEvent() {
  }

  static int childIdOf(long localEvent) {
    return (int) (localEvent & LOW_QUADWORD);
  }

  static int localTimeOf(long localEvent) {
    return (int) (localEvent >> Integer.SIZE);
  }

  static long localEvent(int localTime, int childId) {
    return ((long) localTime << Integer.SIZE) + childId;
  }
}
