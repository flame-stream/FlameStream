package com.spbsu.flamestream.core;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface Rear {
  /**
   * Sync call, return mean accept
   */
  void accept(Batch batch);

  Batch last();
}
