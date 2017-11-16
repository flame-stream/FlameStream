package com.spbsu.flamestream.runtime.utils.serialization;

import com.spbsu.flamestream.runtime.node.tick.api.TickInfo;

public interface TickInfoSerializer {
  byte[] serialize(TickInfo tickInfo);

  TickInfo deserializeTick(byte[] data);
}
