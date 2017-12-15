package com.spbsu.flamestream.runtime.graph.materialization;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

/**
 * User: Artem
 * Date: 13.12.2017
 */
public interface MinTimeHandler {
  void onMinTime(GlobalTime minTime);
}
