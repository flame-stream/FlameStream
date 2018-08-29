package com.spbsu.flamestream.runtime.edge;

import com.spbsu.flamestream.core.Batch;

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
