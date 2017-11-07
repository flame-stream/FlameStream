package com.spbsu.flamestream.core.front;

public interface FrontSubscription {
  void request(long count);

  void cancel();

  void switchToPull();

  void switchToPush();
}
