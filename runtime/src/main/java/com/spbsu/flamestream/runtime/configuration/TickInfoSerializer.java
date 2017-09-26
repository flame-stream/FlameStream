package com.spbsu.flamestream.runtime.configuration;

import com.spbsu.flamestream.core.TickInfo;

public interface TickInfoSerializer {
  byte[] serialize(TickInfo tickInfo);

  TickInfo deserialize(byte[] date);

}
