package com.spbsu.datastream.core.meta;

/**
 * Wrapper class for localEvents
 *
 * Local events are represented as longs, all actions with localEvents should be done with this interfaces
 */
@SuppressWarnings("UtilityClass")
final class LocalEvent {

  private LocalEvent() {
  }

  static int childIdOf(long localEvent) {
    return (int) (localEvent & 0xffffffffl);
  }

  static int localTimeOf(long localEvent) {
    return (int) (localEvent >> Integer.SIZE);
  }

  static long localEvent(int localTime, int childId) {
    return (long) localTime << Integer.SIZE + childId;
  }
}
