package com.spbsu.flamestream.runtime.front;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

/**
 * User: Artem
 * Date: 15.11.2017
 */
public interface FrontHandle {
  void emitDataItem(DataItem dataItem);

  void emitHeartbeat(GlobalTime globalTime);
}
