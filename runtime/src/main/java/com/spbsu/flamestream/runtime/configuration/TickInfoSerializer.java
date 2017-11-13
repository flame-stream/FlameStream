package com.spbsu.flamestream.runtime.configuration;

import com.spbsu.flamestream.runtime.tick.TickInfo;

public interface TickInfoSerializer {
  byte[] serialize(TickInfo tickInfo);

  TickInfo deserializeTick(byte[] data);
}
