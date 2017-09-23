package com.spbsu.flamestream.core.configuration;

import com.spbsu.flamestream.core.tick.TickInfo;

public interface TickInfoSerializer {
  byte[] serialize(TickInfo tickInfo);

  TickInfo deserialize(byte[] date);

}
